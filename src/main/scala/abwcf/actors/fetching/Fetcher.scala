package abwcf.actors.fetching

import abwcf.actors.*
import abwcf.data.{FetchResponse, Page, PageCandidate}
import org.apache.pekko.actor.typed.scaladsl.adapter.TypedActorSystemOps
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.Location
import org.apache.pekko.http.scaladsl.{Http, HttpExt}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * Fetches resources over HTTP. Uses a [[UrlSupplier]] to request URLs from a [[HostQueueRouter]].
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
  case class Fetch(page: Page) extends Command
  case class SetMaxBandwidth(maxBytesPerSec: Int) extends Command
  case object Stop extends Command
  private case class FutureSuccess(value: HttpResponse | ByteString) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
            hostQueueRouter: ActorRef[HostQueue.Command],
            pageGateway: ActorRef[PageGateway.Command],
            urlNormalizer: ActorRef[UrlNormalizer.Command]): Behavior[Command] =
    Behaviors.setup(context => {
      Behaviors.withStash(5)(buffer => {
        val http = Http(context.system.toClassic)
        val materializer = Materializer(context)

        val urlSupplier = context.spawn(
          Behaviors.supervise(UrlSupplier(context.self, hostQueueRouter))
            .onFailure(SupervisorStrategy.restart),
          "url-supplier"
        )

        new Fetcher(crawlDepthLimiter, pageGateway, urlNormalizer, urlSupplier, http, materializer, context, buffer).requestNextUrl()
      })
    })
}

private class Fetcher private (crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
                               pageGateway: ActorRef[PageGateway.Command],
                               urlNormalizer: ActorRef[UrlNormalizer.Command],
                               urlSupplier: ActorRef[UrlSupplier.Command],
                               http: HttpExt,
                               materializer: Materializer,
                               context: ActorContext[Fetcher.Command],
                               buffer: StashBuffer[Fetcher.Command]) {
  import Fetcher.*

  private val config = context.system.settings.config
  private val maxContentLength = config.getBytes("abwcf.fetching.max-content-length")
  private var bytesPerSec = config.getBytes("abwcf.fetching.min-bandwidth-budget-per-fetcher").toInt

  private def requestNextUrl(): Behavior[Command] = {
    //Request a URL from the UrlSupplier:
    urlSupplier ! UrlSupplier.RequestNext

    //Wait for a response from the UrlSupplier:
    Behaviors.receiveMessage({
      case Fetch(page) =>
        context.log.info("Fetching {}", page.url)

        //Send the HTTP request:
        val responseFuture = http.singleRequest(HttpRequest(uri = Uri(page.url))) //TODO: Set Accept header (e.g. "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8")?

        context.pipeToSelf(responseFuture)({
          case Success(response) => FutureSuccess(response)
          case Failure(throwable) => FutureFailure(throwable)
        })

        receiveHttpResponse(page, null)

      case SetMaxBandwidth(maxBytesPerSec) =>
        bytesPerSec = maxBytesPerSec
        Behaviors.same

      case Stop =>
        context.log.debug("Stopping")
        Behaviors.stopped

      case other =>
        context.log.warn("Skipping unexpected message {}", other)
        Behaviors.same
    })
  }

  private def receiveHttpResponse(page: Page, response: HttpResponse): Behavior[Command] = Behaviors.receiveMessage({
    //Handle 4xx and 5xx error responses:
    case FutureSuccess(response: HttpResponse) if response.status.isFailure =>
      context.log.info("Received {} for {}", response.status.toString, page.url)

      //Discard the response entity and notify the PageManager:
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.
      pageGateway ! PageGateway.FetchError(page, response.status)

      buffer.unstashAll(requestNextUrl())

    //Handle 3xx redirection responses:
    case FutureSuccess(response: HttpResponse) if response.status.isRedirection =>
      context.log.info("Received {} for {}", response.status.toString, page.url)

      //Discard the response entity, notify the PageManager and send the redirect URL to the UrlNormalizer:
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.
      val redirectTo = getRedirectUrl(response, page.url)
      pageGateway ! PageGateway.FetchRedirect(page, response.status, redirectTo)
      redirectTo.foreach(url => urlNormalizer ! UrlNormalizer.Normalize(PageCandidate(url, page.crawlDepth))) //The redirect URL should not be fetched immediately as it may already have been processed by the crawler. This also takes care of (shallow) redirect loops. Note that redirection does not increase the crawl depth.

      buffer.unstashAll(requestNextUrl())

    //Handle other HTTP responses:
    case FutureSuccess(response: HttpResponse) =>
      context.log.info("Received {} for {}", response.status.toString, page.url)
      //TODO: What about responses that are not 200 OK?
      //TODO: Check if response.encoding needs to be handled (https://pekko.apache.org/docs/pekko-http/current/common/encoding.html#client-side).

      //Consume the response entity to receive the response body:
      val byteFuture = response.entity //Response entities must be consumed or discarded.
        .withSizeLimit(maxContentLength) //TODO: Catch the EntityStreamSizeException and send a notification downstream.
        .dataBytes
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

      pageGateway ! PageGateway.FetchSuccess(page, new FetchResponse(response, responseBody))
      buffer.unstashAll(requestNextUrl())

    case FutureFailure(throwable) =>
      context.log.error("Exception while fetching {}", page.url, throwable)
      buffer.unstashAll(requestNextUrl())

    case SetMaxBandwidth(maxBytesPerSec) =>
      bytesPerSec = maxBytesPerSec
      Behaviors.same

    case Stop =>
      buffer.stash(Stop) //The Fetcher does not stop while it is actively fetching.
      Behaviors.same

    case other =>
      context.log.warn("Skipping unexpected message {}", other)
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
