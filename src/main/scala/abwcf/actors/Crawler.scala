package abwcf.actors

import abwcf.actors.fetching.FetcherManager
import abwcf.actors.persistence.host.HostPersistenceManager
import abwcf.actors.persistence.page.PagePersistenceManager
import abwcf.data.PageCandidate
import abwcf.persistence.{CoordinatedSlickSession, SlickHostRepository, SlickPageRepository}
import abwcf.util.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{Behavior, SupervisorStrategy}
import org.apache.pekko.stream.Materializer

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
    //Initialize database resources:
    val session = CoordinatedSlickSession.create(context.system)
    val materializer = Materializer.matFromSystem(context.system)
    val hostRepository = new SlickHostRepository(using session, materializer)
    val pageRepository = new SlickPageRepository(using session, materializer)

    val hostPersistenceManager = context.spawn(
      Behaviors.supervise(HostPersistenceManager(hostRepository))
        .onFailure(SupervisorStrategy.resume),
      "host-persistence-manager"
    )

    val pagePersistenceManager = context.spawn(
      Behaviors.supervise(PagePersistenceManager(pageRepository))
        .onFailure(SupervisorStrategy.resume),
      "page-persistence-manager"
    )

    HostManager.initializeSharding(context.system, hostPersistenceManager)
    HostQueue.initializeSharding(context.system)

    val prioritizer = context.spawn(
      Behaviors.supervise(Prioritizer(settings))
        .onFailure(SupervisorStrategy.resume), //The Prioritizer is stateless, so resuming it is safe.
      "prioritizer"
    )

    PageManager.initializeSharding(context.system, pagePersistenceManager, prioritizer)

    val pageRestorer = context.spawn(
      Behaviors.supervise(PageRestorer(pagePersistenceManager))
        .onFailure(SupervisorStrategy.resume), //The PageRestorer is stateless, so resuming it is safe.
      "page-restorer"
    )

    val userCodeRunner = context.spawn(
      Behaviors.supervise(UserCodeRunner(settings))
        .onFailure(SupervisorStrategy.resume), //The UserCodeRunner is stateless, so resuming it is safe.
      "user-code-runner"
    )

    val robotsFilter = context.spawn(
      Behaviors.supervise(RobotsFilter())
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
      Behaviors.supervise(FetcherManager(crawlDepthLimiter, hostQueueRouter, urlNormalizer, userCodeRunner))
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
