package abwcf.actors

import abwcf.util.HttpUtils
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.adapter.TypedActorSystemOps
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityRef}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpRequest, HttpResponse, Uri}
import org.apache.pekko.http.scaladsl.{Http, HttpExt}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * Fetches a single `robots.txt` file and sends the result to the corresponding [[HostManager]].
 *
 * [[RobotsFetcher]] actors should be managed by a [[RobotsFetcherManager]] actor.
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

  def apply(schemeAndAuthority: String): Behavior[Command] = Behaviors.setup(context => {
    val http = Http(context.system.toClassic)
    val materializer = Materializer(context)
    val hostManager = ClusterSharding(context.system).entityRefFor(HostManager.TypeKey, schemeAndAuthority)

    new RobotsFetcher(schemeAndAuthority, hostManager, http, materializer, context).sendRequest(schemeAndAuthority + "/robots.txt", 0)
  })
}

private class RobotsFetcher private (schemeAndAuthority: String,
                                     hostManager: EntityRef[HostManager.Command],
                                     http: HttpExt,
                                     materializer: Materializer,
                                     context: ActorContext[RobotsFetcher.Command]) {
  import RobotsFetcher.*

  private val config = context.system.settings.config
  private val maxContentLength = config.getBytes("abwcf.robots.fetching.max-content-length")
  private val bytesPerSec = config.getBytes("abwcf.robots.fetching.bandwidth-budget-per-file").toInt
  private val maxRedirects = config.getInt("abwcf.robots.fetching.max-redirects")

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
        .throttle(bytesPerSec, 1 second, bytes => bytes.length)
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
          hostManager ! HostManager.Unavailable
          Behaviors.stopped
      }

    //Handle 4xx error responses:
    case FutureSuccess(response: HttpResponse) if 400 to 499 contains response.status.intValue =>
      context.log.info("Received {} for {}", response.status.toString, url)
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.
      hostManager ! HostManager.Unavailable
      Behaviors.stopped

    //Handle other HTTP responses:
    case FutureSuccess(response: HttpResponse) =>
      context.log.info("Received {} for {}", response.status.toString, url)
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.
      hostManager ! HostManager.Unreachable
      Behaviors.stopped

    //Send the complete response to the HostManager after the response body has been received:
    case FutureSuccess(responseBody: ByteString) =>
      context.log.info("Received {} bytes for {}", responseBody.length, url)
      hostManager ! HostManager.Response(responseBody)
      Behaviors.stopped

    case FutureFailure(throwable) =>
      context.log.error("Exception while fetching {}", url, throwable)
      hostManager ! HostManager.Unreachable
      Behaviors.stopped
  })
}
