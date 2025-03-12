package abwcf.actors

import org.apache.pekko.actor.typed.scaladsl.adapter.TypedActorSystemOps
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import org.apache.pekko.http.scaladsl.{Http, HttpExt}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.{ByteString, Timeout}

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * Fetches resources over HTTP.
 *
 * [[Fetcher]] actors should be managed by a [[FetcherManager]] actor.
 *
 * This actor is stateful.
 */
object Fetcher {
  sealed trait Command
  case object Stop extends Command
  private case object AskFailure extends Command
  private case class FutureSuccess(value: HttpResponse | ByteString) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  private type CombinedCommand = Command | HostQueue.Reply

  def apply(hostQueueRouter: ActorRef[HostQueue.Command]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    Behaviors.withStash(5)(buffer => {
      val http = Http(context.system.toClassic)
      val materializer = Materializer(context)

      //TODO: Add these to config:
      val urlRequestTimeout = 3 seconds
      val bytesPerSec = 1_000_000 //Don't make this value too small.

      new Fetcher(hostQueueRouter, http, materializer, urlRequestTimeout, bytesPerSec, context, buffer).requestNextUrl()
    })
  }).narrow
}

private class Fetcher private (hostQueueRouter: ActorRef[HostQueue.Command],
                               http: HttpExt,
                               materializer: Materializer,
                               urlRequestTimeout: Timeout,
                               bytesPerSec: Int,
                               context: ActorContext[Fetcher.CombinedCommand],
                               buffer: StashBuffer[Fetcher.CombinedCommand]) {
  import Fetcher.*

  private def requestNextUrl(): Behavior[CombinedCommand] = {
    //Request a URL from the HostQueueRouter:
    context.ask(hostQueueRouter, HostQueue.GetHead.apply)({
      case Success(reply) => reply
      case Failure(_) => AskFailure
    })(urlRequestTimeout)

    //Handle the reply from the HostQueue:
    Behaviors.receiveMessage({
      case HostQueue.Head(url) =>
        context.log.info("Fetching {}", url)

        val responseFuture = http.singleRequest(HttpRequest(uri = Uri(url)))

        context.pipeToSelf(responseFuture)({
          case Success(response) => FutureSuccess(response)
          case Failure(throwable) => FutureFailure(throwable)
        })

        receiveHttpResponse(url)

      case HostQueue.Unavailable | AskFailure =>
        requestNextUrl()

      case Stop =>
        context.log.debug("Stopping")
        Behaviors.stopped

      case other =>
        context.log.info("Skipping unexpected message {}", other)
        Behaviors.same
    })
  }

  private def receiveHttpResponse(url: String): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case FutureSuccess(response: HttpResponse) => //The response entity must be consumed!
      context.log.info(response.status.toString) //TODO: Handle status codes.

      val byteFuture = response.entity.dataBytes
        .throttle(bytesPerSec, 1 second, bytes => bytes.length) //Throttles the stream and the download. The effect on network usage probably also depends on other factors (e.g. drivers) and is more noticeable during long downloads (e.g. 10 MB at 100 kB/s).
        .runReduce(_ ++ _)(using materializer)

      context.pipeToSelf(byteFuture)({
        case Success(byteString) => FutureSuccess(byteString)
        case Failure(throwable) => FutureFailure(throwable)
      })

      Behaviors.same

    case FutureSuccess(byteString: ByteString) =>
      context.log.info("Received {} bytes for {}", byteString.length, url)
      buffer.unstashAll(requestNextUrl())

    case FutureFailure(throwable) =>
      context.log.error("Error while fetching {}", url, throwable)
      buffer.unstashAll(requestNextUrl())

    case Stop =>
      buffer.stash(Stop)
      Behaviors.same

    case other =>
      context.log.info("Skipping unexpected message {}", other)
      Behaviors.same
  })
}
