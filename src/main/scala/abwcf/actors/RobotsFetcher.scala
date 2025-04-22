package abwcf.actors

import abwcf.util.HttpUtils
import org.apache.pekko.actor.typed.scaladsl.adapter.TypedActorSystemOps
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpRequest, HttpResponse, Uri}
import org.apache.pekko.http.scaladsl.{Http, HttpExt}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * Fetches the `robots.txt` file for a [[HostManager]].
 *
 * There should be one [[RobotsFetcher]] actor per [[HostManager]] actor.
 *
 * This actor is stateful.
 *
 * @see
 *      - [[https://datatracker.ietf.org/doc/html/rfc9309 RFC 9309 - Robots Exclusion Protocol]]
 *      - [[https://en.wikipedia.org/wiki/Robots.txt Wikipedia: robots.txt]]
 */
object RobotsFetcher {
  sealed trait Command
  private case class FutureSuccess(value: HttpResponse | ByteString) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  sealed trait Reply
  case class Response(responseBody: ByteString) extends Reply
  case object Unavailable extends Reply
  case object Unreachable extends Reply

  def apply(schemeAndAuthority: String, replyTo: ActorRef[Reply]): Behavior[Command] = Behaviors.setup(context => { //The RobotsFetcher is spawned by the HostManager, so using an ActorRef (instead of a ShardingEnvelope) for the reply is not an issue.
    val http = Http(context.system.toClassic)
    val materializer = Materializer(context)

    new RobotsFetcher(schemeAndAuthority, replyTo, http, materializer, context).sendRequest(schemeAndAuthority + "/robots.txt", 0)
  })
}

private class RobotsFetcher private (schemeAndAuthority: String,
                                     replyTo: ActorRef[RobotsFetcher.Reply],
                                     http: HttpExt,
                                     materializer: Materializer,
                                     context: ActorContext[RobotsFetcher.Command]) {
  import RobotsFetcher.*

  private val config = context.system.settings.config
  private val maxContentLength = config.getBytes("abwcf.actors.robots-fetcher.max-content-length")
  private val bytesPerSec = config.getBytes("abwcf.actors.robots-fetcher.bandwidth-budget-per-file").toInt
  private val maxRedirects = config.getInt("abwcf.actors.robots-fetcher.max-redirects")

  private def sendRequest(url: String, redirectCounter: Int): Behavior[Command] = {
    context.log.info("Fetching {}", url)

    //Send the HTTP request:
    val responseFuture = http.singleRequest(HttpRequest(uri = Uri(url)))

    context.pipeToSelf(responseFuture)({
      case Success(response) => FutureSuccess(response)
      case Failure(throwable) => FutureFailure(throwable)
    })

    receiveHttpResponse(url, null, redirectCounter)
  }

  private def receiveHttpResponse(url: String, response: HttpResponse, redirectCounter: Int): Behavior[Command] = Behaviors.receiveMessage({
    //Handle successful responses with the correct media type and encoding (as specified by RFC 9309):
    case FutureSuccess(response: HttpResponse) if response.status.isSuccess && !response.status.isRedirection && response.entity.contentType == ContentTypes.`text/plain(UTF-8)` =>
      context.log.info("Received {} for {}", response.status.toString, url)

      //Consume the response entity to receive the response body:
      val byteFuture = response.entity //Response entities must be consumed or discarded.
        .dataBytes
        .throttle(bytesPerSec, 1 second, bytes => bytes.length) //TODO: Total bandwidth budget for all RobotsFetchers?
        .flatMapConcat(Source(_))
        .take(maxContentLength)
        .fold(ByteString.newBuilder)((builder, byte) => builder.addOne(byte)) //Rebuild the ByteString.
        .map(_.result())
        .runWith(Sink.head)(using materializer)

      context.pipeToSelf(byteFuture)({
        case Success(byteString) => FutureSuccess(byteString)
        case Failure(throwable) => FutureFailure(throwable)
      })

      receiveHttpResponse(url, response, redirectCounter)

    //Handle 3xx redirection responses:
    case FutureSuccess(response: HttpResponse) if response.status.isRedirection =>
      context.log.info("Received {} for {}", response.status.toString, url)
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.

      //Follow the redirection:
      HttpUtils.getRedirectUrl(response, url) match {
        case Some(redirectUrl) if redirectCounter < maxRedirects =>
          sendRequest(redirectUrl, redirectCounter + 1)

        case _ =>
          replyTo ! Unavailable
          Behaviors.stopped
      }

    //Handle 4xx error responses:
    case FutureSuccess(response: HttpResponse) if 400 to 499 contains response.status.intValue =>
      context.log.info("Received {} for {}", response.status.toString, url)
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.
      replyTo ! Unavailable
      Behaviors.stopped

    //Handle other HTTP responses:
    case FutureSuccess(response: HttpResponse) =>
      context.log.info("Received {} for {}", response.status.toString, url)
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.
      replyTo ! Unreachable
      Behaviors.stopped

    //Send the complete response to the HostManager after the response body has been received:
    case FutureSuccess(responseBody: ByteString) =>
      context.log.info("Received {} bytes for {}", responseBody.length, url)
      replyTo ! Response(responseBody)
      Behaviors.stopped

    case FutureFailure(throwable) =>
      context.log.error("Exception while fetching {}", url, throwable)
      replyTo ! Unreachable
      Behaviors.stopped
  })
}
