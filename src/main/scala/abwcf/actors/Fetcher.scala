package abwcf.actors

import abwcf.{FetchResponse, PageEntity, PageStatus}
import org.apache.pekko.actor.typed.scaladsl.adapter.TypedActorSystemOps
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.Location
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
  /**
   * Media types that can be parsed by the [[HtmlParser]] actor.
   */
  private val ParseableMediaTypes = List(MediaTypes.`text/html`, MediaTypes.`application/xhtml+xml`)

  sealed trait Command
  case object Stop extends Command
  private case object AskFailure extends Command
  private case class FutureSuccess(value: HttpResponse | ByteString) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  private type CombinedCommand = Command | HostQueue.Reply

  def apply(crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
            hostQueueRouter: ActorRef[HostQueue.Command],
            pageManager: ActorRef[PageManager.Command],
            urlNormalizer: ActorRef[UrlNormalizer.Command]): Behavior[Command] =
    Behaviors.setup[CombinedCommand](context => {
      Behaviors.withStash(5)(buffer => {
        val http = Http(context.system.toClassic)
        val materializer = Materializer(context)

        //TODO: Add these to config:
        val urlRequestTimeout = 3 seconds
        val bytesPerSec = 1_000_000 //Don't make this value too small.

        new Fetcher(crawlDepthLimiter, hostQueueRouter, pageManager, urlNormalizer, http, materializer, urlRequestTimeout, bytesPerSec, context, buffer).requestNextUrl()
      })
    }).narrow
}

private class Fetcher private (crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
                               hostQueueRouter: ActorRef[HostQueue.Command],
                               pageManager: ActorRef[PageManager.Command],
                               urlNormalizer: ActorRef[UrlNormalizer.Command],
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
      case HostQueue.Head(page) =>
        context.log.info("Fetching {}", page.url)

        //Send the HTTP request:
        val responseFuture = http.singleRequest(HttpRequest(uri = Uri(page.url))) //TODO: Set Accept header (e.g. "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8")?

        context.pipeToSelf(responseFuture)({
          case Success(response) => FutureSuccess(response)
          case Failure(throwable) => FutureFailure(throwable)
        })

        receiveHttpResponse(page, null)

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

  private def receiveHttpResponse(page: PageEntity, response: HttpResponse): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    //Handle 4xx and 5xx error responses:
    case FutureSuccess(response: HttpResponse) if response.status.isFailure =>
      context.log.info("Received {} for {}", response.status.toString, page.url)

      //Discard the response entity and notify the Page:
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.
      pageManager ! PageManager.FetchError(page, response.status)

      buffer.unstashAll(requestNextUrl())

    //Handle 3xx redirection responses:
    case FutureSuccess(response: HttpResponse) if response.status.isRedirection =>
      context.log.info("Received {} for {}", response.status.toString, page.url)

      //Discard the response entity, notify the Page and send the redirect URL to the UrlNormalizer:
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.
      val redirectTo = getRedirectUrl(response, page.url)
      pageManager ! PageManager.FetchRedirect(page, response.status, redirectTo)
      redirectTo.foreach(url => urlNormalizer ! UrlNormalizer.Normalize(PageEntity(url, PageStatus.Unknown, page.crawlDepth))) //The redirect URL should not be fetched immediately as it may already have been processed by the crawler.

      buffer.unstashAll(requestNextUrl())

    //Handle other HTTP responses:
    case FutureSuccess(response: HttpResponse) =>
      context.log.info("Received {} for {}", response.status.toString, page.url)
      //TODO: What about responses that are not 200 OK?
      //TODO: Check if response.encoding needs to be handled (https://pekko.apache.org/docs/pekko-http/current/common/encoding.html#client-side).

      //Consume the response entity to receive the response body:
      val byteFuture = response.entity.dataBytes //Response entities must be consumed or discarded.
        .throttle(bytesPerSec, 1 second, bytes => bytes.length) //Throttles the stream and the download. The effect on network usage probably also depends on other factors (e.g. drivers) and is more noticeable during long downloads (e.g. 10 MB at 100 kB/s).
        .runReduce(_ ++ _)(using materializer)

      context.pipeToSelf(byteFuture)({
        case Success(byteString) => FutureSuccess(byteString)
        case Failure(throwable) => FutureFailure(throwable)
      })

      receiveHttpResponse(page, response)

    //Send the complete response downstream after the response body has been received:
    case FutureSuccess(responseBody: ByteString) =>
      context.log.info("Received {} bytes ({}) for {}", responseBody.length, response.entity.contentType, page.url)

      if (ParseableMediaTypes.contains(response.entity.contentType.mediaType)) { //Possible alternative: Use mediaType.binary or mediaType.isText.
        crawlDepthLimiter ! CrawlDepthLimiter.CheckDepth(page, responseBody)
      }

      pageManager ! PageManager.FetchSuccess(page, new FetchResponse(response, responseBody))
      buffer.unstashAll(requestNextUrl())

    case FutureFailure(throwable) =>
      context.log.error("Error while fetching {}", page.url, throwable)
      buffer.unstashAll(requestNextUrl())

    case Stop =>
      buffer.stash(Stop)
      Behaviors.same

    case other =>
      context.log.info("Skipping unexpected message {}", other)
      Behaviors.same
  })

  /**
   * Returns the redirection URL from the `Location` response header.
   *
   * @param response the response from the server
   * @param url the URL of the original request
   *
   * @see
   *      - [[https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Location MDN: Location]]
   *      - [[https://datatracker.ietf.org/doc/html/rfc9110 RFC 9110 - HTTP Semantics]] (section ''10.2.2. Location'')
   */
  private def getRedirectUrl(response: HttpResponse, url: String): Option[String] = {
    val locationHeaders = response.headers[Location]

    if (locationHeaders.nonEmpty) {
      var redirectUri = locationHeaders.head.uri

      if (redirectUri.isRelative) {
        val originalUri = Uri(url)
        redirectUri = redirectUri.resolvedAgainst(originalUri).withFragment(originalUri.fragment.orNull)
      }

      Some(redirectUri.toString)
    } else {
      None
    }
  }
}
