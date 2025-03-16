package abwcf.actors

import org.apache.pekko.actor.typed.scaladsl.adapter.TypedActorSystemOps
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
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

  def apply(hostQueueRouter: ActorRef[HostQueue.Command], crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    Behaviors.withStash(5)(buffer => {
      val http = Http(context.system.toClassic)
      val materializer = Materializer(context)
      val pageShardRegion = Page.getShardRegion(context.system)

      //TODO: Add these to config:
      val urlRequestTimeout = 3 seconds
      val bytesPerSec = 1_000_000 //Don't make this value too small.

      new Fetcher(hostQueueRouter, crawlDepthLimiter, pageShardRegion, http, materializer, urlRequestTimeout, bytesPerSec, context, buffer).requestNextUrl()
    })
  }).narrow
}

private class Fetcher private (hostQueueRouter: ActorRef[HostQueue.Command],
                               crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
                               pageShardRegion: ActorRef[ShardingEnvelope[Page.Command]],
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

        //Send the HTTP request:
        val responseFuture = http.singleRequest(HttpRequest(uri = Uri(url)))

        context.pipeToSelf(responseFuture)({
          case Success(response) => FutureSuccess(response)
          case Failure(throwable) => FutureFailure(throwable)
        })

        receiveHttpResponse(url, null)

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

  private def receiveHttpResponse(url: String, response: HttpResponse): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case FutureSuccess(response: HttpResponse) => //The response entity must be consumed!
      context.log.info("Received {} for {}", response.status.toString, url)
      //TODO: Handle status codes and content types.
//      response.entity.contentType == ContentTypes.`text/html(UTF-8)`

      //Receive the response body:
      val byteFuture = response.entity.dataBytes
        .throttle(bytesPerSec, 1 second, bytes => bytes.length) //Throttles the stream and the download. The effect on network usage probably also depends on other factors (e.g. drivers) and is more noticeable during long downloads (e.g. 10 MB at 100 kB/s).
        .runReduce(_ ++ _)(using materializer)

      context.pipeToSelf(byteFuture)({
        case Success(byteString) => FutureSuccess(byteString)
        case Failure(throwable) => FutureFailure(throwable)
      })

      receiveHttpResponse(url, response)

    case FutureSuccess(responseBody: ByteString) =>
      context.log.info("Received {} bytes ({}) for {}", responseBody.length, response.entity.contentType, url)
      
      //Send the response data to the CrawlDepthLimiter and the Page:
      crawlDepthLimiter ! CrawlDepthLimiter.CheckDepth(url, responseBody)
      pageShardRegion ! ShardingEnvelope(url, Page.FetchSuccess(response, responseBody))
      
      buffer.unstashAll(requestNextUrl())

    case FutureFailure(throwable) =>
      context.log.error("Error while fetching {}", url, throwable)
      
      //Notify the Page:
      pageShardRegion ! ShardingEnvelope(url, Page.FetchFailure)
      
      buffer.unstashAll(requestNextUrl())

    case Stop =>
      buffer.stash(Stop)
      Behaviors.same

    case other =>
      context.log.info("Skipping unexpected message {}", other)
      Behaviors.same
  })
}
