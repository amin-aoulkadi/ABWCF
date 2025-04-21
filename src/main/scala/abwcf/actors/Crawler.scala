package abwcf.actors

import abwcf.actors.fetching.FetcherManager
import abwcf.actors.persistence.host.HostPersistenceManager
import abwcf.data.PageCandidate
import abwcf.persistence.SlickSessionContainer
import abwcf.util.{ActorRegistry, CrawlerSettings}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{Behavior, SupervisorStrategy}

import java.net.URISyntaxException

/**
 * The user guardian actor for the ABWCF.
 *
 * There should be exactly one [[Crawler]] actor per node.
 */
object Crawler {
  sealed trait Command
  case class SeedUrls(urls: Seq[String]) extends Command

  def apply(settings: CrawlerSettings = CrawlerSettings()): Behavior[Command] = Behaviors.setup(context => {
    SlickSessionContainer.initialize(context.system) //Initialize database connection resources.

    val hostPersistenceManager = context.spawn(
      Behaviors.supervise(HostPersistenceManager())
        .onFailure(SupervisorStrategy.resume),
      "host-persistence-manager"
    )

    ActorRegistry.hostPersistenceManager = Some(hostPersistenceManager)

    val pageGateway = context.spawn(
      Behaviors.supervise(PageGateway(settings))
        .onFailure(SupervisorStrategy.resume), //The PageGateway is stateless, so resuming it is safe.
      "page-gateway"
    )

    val robotsFilter = context.spawn(
      Behaviors.supervise(RobotsFilter(pageGateway))
        .onFailure(SupervisorStrategy.resume), //Restarting would mean losing all pending candidates and repopulating the cache (which is rather expensive).
      "robots-filter"
    )

    val urlFilter = context.spawn(
      Behaviors.supervise(UrlFilter(robotsFilter))
        .onFailure(SupervisorStrategy.resume), //The UrlFilter is stateless, so resuming it is safe.
      "url-filter"
    )

    val urlNormalizer = context.spawn(
      Behaviors.supervise(UrlNormalizer(urlFilter))
        .onFailure[URISyntaxException](SupervisorStrategy.resume), //The UrlNormalizer is stateless, so resuming it is safe.
      "url-normalizer"
    )

    val htmlParser = context.spawn(
      Behaviors.supervise(HtmlParser(urlNormalizer))
        .onFailure(SupervisorStrategy.resume), //The HtmlParser is stateless, so resuming it is safe.
      "html-parser"
    )

    val hostQueueRouter = context.spawn(
      Behaviors.supervise(HostQueueRouter())
        .onFailure(SupervisorStrategy.resume), //The HostQueueRouter is stateless, so resuming it should be safe.
      "host-queue-router"
    )

    val crawlDepthLimiter = context.spawn(
      Behaviors.supervise(CrawlDepthLimiter(htmlParser))
        .onFailure(SupervisorStrategy.resume), //The CrawlDepthLimiter is stateless, so resuming it is safe.
      "crawl-depth-limiter"
    )

    val fetcherManager = context.spawn(
      Behaviors.supervise(FetcherManager(crawlDepthLimiter, hostQueueRouter, pageGateway, urlNormalizer))
        .onFailure(SupervisorStrategy.restart),
      "fetcher-manager"
    )

    Behaviors.receiveMessage({
      case SeedUrls(urls) =>
        urls.foreach(url => urlNormalizer ! UrlNormalizer.Normalize(PageCandidate(url, 0)))
        Behaviors.same
    })
  })
}
