package abwcf.actors.fetching

import abwcf.actors.*
import abwcf.data.{FetchResponse, Page, PageCandidate}
import abwcf.metrics.FetcherMetrics
import abwcf.util.{CrawlerSettings, HttpUtils}
import org.apache.pekko.actor.typed.scaladsl.adapter.TypedActorSystemOps
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.http.scaladsl.model.*
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
   * Media types that may contain links that can be parsed by the [[HtmlParser]] actor.
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
            urlNormalizer: ActorRef[UrlNormalizer.Command],
            userCodeRunner: ActorRef[UserCodeRunner.Command],
            settings: CrawlerSettings): Behavior[Command] =
    Behaviors.setup(context => {
      Behaviors.withStash(5)(buffer => {
        val http = Http(context.system.toClassic)
        val materializer = Materializer(context)

        val urlSupplier = context.spawn(
          Behaviors.supervise(UrlSupplier(context.self, hostQueueRouter))
            .onFailure(SupervisorStrategy.restart),
          "url-supplier"
        )

        new Fetcher(crawlDepthLimiter, urlNormalizer, urlSupplier, userCodeRunner, http, materializer, settings, context, buffer).requestNextUrl()
      })
    })
}

private class Fetcher private (crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
                               urlNormalizer: ActorRef[UrlNormalizer.Command],
                               urlSupplier: ActorRef[UrlSupplier.Command],
                               userCodeRunner: ActorRef[UserCodeRunner.Command],
                               http: HttpExt,
                               materializer: Materializer,
                               settings: CrawlerSettings,
                               context: ActorContext[Fetcher.Command],
                               buffer: StashBuffer[Fetcher.Command]) {
  import Fetcher.*

  private val config = context.system.settings.config
  private val maxContentLength = config.getBytes("abwcf.fetching.max-content-length")
  private var bytesPerSec = config.getBytes("abwcf.fetching.min-bandwidth-budget-per-fetcher").toInt //Mutable state!
  private val metrics = FetcherMetrics(settings, context)

  private def requestNextUrl(): Behavior[Command] = {
    //Request a URL from the UrlSupplier:
    urlSupplier ! UrlSupplier.RequestNext

    //Wait for a response from the UrlSupplier:
    Behaviors.receiveMessage({
      case Fetch(page) =>
        context.log.info("Fetching {}", page.url)
        metrics.addFetchedPages(1)

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
      metrics.addResponse(response)

      //Discard the response entity and notify the UserCodeRunner:
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.
      userCodeRunner ! UserCodeRunner.Error(page, response.status)

      buffer.unstashAll(requestNextUrl())

    //Handle 3xx redirection responses:
    case FutureSuccess(response: HttpResponse) if response.status.isRedirection =>
      context.log.info("Received {} for {}", response.status.toString, page.url)
      metrics.addResponse(response)

      //Discard the response entity, notify the UserCodeRunner and send the redirect URL to the UrlNormalizer:
      response.discardEntityBytes(materializer) //Response entities must be consumed or discarded.
      val redirectTo = HttpUtils.getRedirectUrl(response, page.url)
      userCodeRunner ! UserCodeRunner.Redirect(page, response.status, redirectTo)
      redirectTo.foreach(url => urlNormalizer ! UrlNormalizer.Normalize(PageCandidate(url, page.crawlDepth))) //The redirect URL should not be fetched immediately as it may already have been processed by the crawler. This also takes care of (shallow) redirect loops. Note that redirection does not increase the crawl depth.

      buffer.unstashAll(requestNextUrl())

    //Handle other HTTP responses:
    case FutureSuccess(response: HttpResponse) =>
      context.log.info("Received {} for {}", response.status.toString, page.url)
      metrics.addResponse(response)
      //TODO: What about responses that are not 200 OK?
      //TODO: Check if response.encoding needs to be handled (https://pekko.apache.org/docs/pekko-http/current/common/encoding.html#client-side).

      //Consume the response entity to receive the response body:
      val byteFuture = response.entity //Response entities must be consumed or discarded.
        .withSizeLimit(maxContentLength) //Throws an EntityStreamSizeException if the response body is too long.
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
      metrics.addReceivedBytes(responseBody.length)
      val fetchResponse = new FetchResponse(response, responseBody)

      if (ParseableMediaTypes.contains(response.entity.contentType.mediaType)) { //Possible alternative: Use mediaType.binary or mediaType.isText.
        crawlDepthLimiter ! CrawlDepthLimiter.CheckDepth(page, fetchResponse)
      }

      userCodeRunner ! UserCodeRunner.Success(page, fetchResponse)
      buffer.unstashAll(requestNextUrl())

		//Notify the UserCodeRunner if the response body exceeds the maximum accepted content length:
    case FutureFailure(_: EntityStreamSizeException) =>
      userCodeRunner ! UserCodeRunner.LengthLimitExceeded(page, new FetchResponse(response, ByteString.empty))
      buffer.unstashAll(requestNextUrl())

    case FutureFailure(throwable) =>
      context.log.error("Exception while fetching {}", page.url, throwable)
      metrics.addException(throwable)
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
}
