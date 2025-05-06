package abwcf.actors

import abwcf.actors.fetching.FetcherManager
import abwcf.actors.metrics.ClusterNodeMetricsCollector
import abwcf.actors.persistence.host.HostPersistenceManager
import abwcf.actors.persistence.page.PagePersistenceManager
import abwcf.data.PageCandidate
import abwcf.persistence.{CoordinatedSlickSession, SlickHostRepository, SlickPageRepository}
import abwcf.util.CrawlerSettings
import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{Behavior, SupervisorStrategy}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import java.net.URISyntaxException
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * The user guardian actor for the ABWCF.
 *
 * There should be exactly one [[Crawler]] actor per node.
 */
object Crawler {
  sealed trait Command
  case class SeedUrls(urls: Seq[String]) extends Command

  def apply(settings: CrawlerSettings = CrawlerSettings()): Behavior[Command] = Behaviors.setup(context => {
    val system = context.system
    val materializer = Materializer.matFromSystem(system)

    //Initialize database resources:
    val session = CoordinatedSlickSession.create(system)
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

    val robotsFetcherManager = context.spawn(
      Behaviors.supervise(RobotsFetcherManager())
        .onFailure(SupervisorStrategy.resume),
      "robots-fetcher-manager"
    )

    val prioritizer = context.spawn(
      Behaviors.supervise(Prioritizer(settings))
        .onFailure(SupervisorStrategy.resume), //The Prioritizer is stateless, so resuming it is safe.
      "prioritizer"
    )

    //Initialize shard regions:
    HostManager.initializeSharding(system, hostPersistenceManager, robotsFetcherManager)
    HostQueue.initializeSharding(system)
    PageManager.initializeSharding(system, pagePersistenceManager, prioritizer)

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
      Behaviors.supervise(UrlNormalizer(urlFilter, settings))
        .onFailure[URISyntaxException](SupervisorStrategy.resume), //The UrlNormalizer is stateless, so resuming it is safe.
      "url-normalizer"
    )

    val htmlParser = context.spawn(
      Behaviors.supervise(HtmlParser(urlNormalizer))
        .onFailure(SupervisorStrategy.resume), //The HtmlParser is stateless, so resuming it is safe.
      "html-parser"
    )

    val robotsHeaderFilter = context.spawn(
      Behaviors.supervise(RobotsHeaderFilter(htmlParser))
        .onFailure(SupervisorStrategy.resume), //The RobotsHeaderFilter is stateless, so resuming it is safe.
      "robots-header-filter"
    )

    val hostQueueRouter = context.spawn(
      Behaviors.supervise(HostQueueRouter())
        .onFailure(SupervisorStrategy.resume), //The HostQueueRouter is stateless, so resuming it should be safe.
      "host-queue-router"
    )

    val crawlDepthLimiter = context.spawn(
      Behaviors.supervise(CrawlDepthLimiter(robotsHeaderFilter))
        .onFailure(SupervisorStrategy.resume), //The CrawlDepthLimiter is stateless, so resuming it is safe.
      "crawl-depth-limiter"
    )

    val fetcherManager = context.spawn(
      Behaviors.supervise(FetcherManager(crawlDepthLimiter, hostQueueRouter, urlNormalizer, userCodeRunner))
        .onFailure(SupervisorStrategy.restart),
      "fetcher-manager"
    )

    context.spawn(
      Behaviors.supervise(ClusterNodeMetricsCollector(settings))
        .onFailure(SupervisorStrategy.restart),
      "cluster-node-metrics-aggregator"
    )

    //Add coordinated shutdown tasks:
    CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "shutdown-data-sources")(() => {
      //Stop the Fetchers and the PageRestorer so that they stop introducing new data into the system:
      val timeout = Timeout(15 seconds)
      val fetcherManagerFuture = fetcherManager.ask(FetcherManager.Shutdown.apply)(using timeout, system.scheduler)
      val pageRestorerFuture = pageRestorer.ask(PageRestorer.Shutdown.apply)(using timeout, system.scheduler)
      fetcherManagerFuture.zipWith(pageRestorerFuture)((_, _) => Done)(using system.executionContext)
    })

    Behaviors.receiveMessage({
      case SeedUrls(urls) =>
        urls.foreach(url => urlNormalizer ! UrlNormalizer.Normalize(PageCandidate(url, 0)))
        Behaviors.same
    })
  })
}
