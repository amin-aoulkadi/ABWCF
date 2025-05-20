package abwcf.actors

import abwcf.api.CrawlerSettings
import abwcf.data.{HostInformation, PageCandidate}
import abwcf.metrics.LenientRobotsFilterMetrics
import abwcf.util.UrlUtils
import com.github.benmanes.caffeine.cache.{Caffeine, Expiry}
import crawlercommons.robots.SimpleRobotRules
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding
import org.apache.pekko.util.Timeout

import java.time.Instant
import scala.collection.mutable
import scala.jdk.DurationConverters.*
import scala.util.{Failure, Success}

/**
 * Filters out URLs that should not be crawled based on the Robots Exclusion Protocol.
 *
 * URLs are sent downstream unfiltered if the required [[HostInformation]] is currently unavailable (e.g. if the corresponding `robots.txt` file has not been fetched yet).
 *
 * There should be one [[LenientRobotsFilter]] actor per node.
 *
 * This actor is stateful.
 *
 * @see
 *      - [[https://datatracker.ietf.org/doc/html/rfc9309 RFC 9309 - Robots Exclusion Protocol]]
 *      - [[https://en.wikipedia.org/wiki/Robots.txt Wikipedia: robots.txt]]
 */
object LenientRobotsFilter {
  sealed trait Command
  case class Filter(candidate: PageCandidate) extends Command

  private type CombinedCommand = Command | HostManager.HostInfo

  def apply(settings: CrawlerSettings): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    new LenientRobotsFilter(settings, context).lenientRobotsFilter()
  }).narrow
}

private class LenientRobotsFilter private (settings: CrawlerSettings, context: ActorContext[LenientRobotsFilter.CombinedCommand]) {
  import LenientRobotsFilter.*

  private val sharding = ClusterSharding(context.system)
  private val config = context.system.settings.config
  private val askTimeout = config.getDuration("abwcf.actors.lenient-robots-filter.ask-timeout").toScala
  private val failOpenDuration = config.getDuration("abwcf.actors.lenient-robots-filter.fail-open-duration")
  private val maxCacheSize = config.getLong("abwcf.actors.lenient-robots-filter.max-cache-size")

  private val hostInfoCache = Caffeine.newBuilder() //Mutable state!
    .maximumSize(maxCacheSize)
    .expireAfter(Expiry.writing[String, HostInformation]((_, hostInfo) => Instant.now.until(hostInfo.validUntil)))
    .recordStats() //Needed for metrics.
    .build[String, HostInformation]()

  /**
   * Buffers URLs while the request for the required [[HostInformation]] is in progress.
   */
  private val pendingCandidates = mutable.HashMap.empty[String, mutable.ArrayBuffer[PageCandidate]] //Mutable state!

  private val metrics = LenientRobotsFilterMetrics(settings, context, hostInfoCache)

  private def lenientRobotsFilter(): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case Filter(candidate) =>
      val schemeAndAuthority = UrlUtils.getSchemeAndAuthority(candidate.url)
      val hostInfo = hostInfoCache.getIfPresent(schemeAndAuthority)

      if (hostInfo == null) { //Cache miss
        //Buffer the URL:
        val candidates = pendingCandidates.getOrElse(schemeAndAuthority, mutable.ArrayBuffer.empty)
        pendingCandidates.update(schemeAndAuthority, candidates.append(candidate))

        //Request information from the HostManager:
        if (candidates.size == 1) { //If the buffer contains more than one element at this point, then a request is already in progress.
          val hostManager = sharding.entityRefFor(HostManager.TypeKey, schemeAndAuthority)

          context.ask(hostManager, HostManager.GetHostInfo.apply)({
            case Success(reply) => reply
            case Failure(_) =>
              //Fall back to HostInformation with rules that allow everything:
              val hostInfo = HostInformation(schemeAndAuthority, new SimpleRobotRules(RobotRulesMode.ALLOW_ALL), Instant.now.plus(failOpenDuration))
              HostManager.HostInfo(hostInfo)
          })(using askTimeout)
        }
      } else { //Cache hit
        filterPageCandidate(candidate, hostInfo)
      }

      Behaviors.same

    case HostManager.HostInfo(hostInfo) =>
      hostInfoCache.put(hostInfo.schemeAndAuthority, hostInfo)

      //Filter the buffered URLs:
      pendingCandidates.remove(hostInfo.schemeAndAuthority)
        .getOrElse(mutable.ArrayBuffer.empty)
        .foreach(candidate => filterPageCandidate(candidate, hostInfo))

      Behaviors.same
  })

  /**
   * Checks if the [[PageCandidate]] is allowed to be crawled based on the Robots Exclusion Protocol.
   *
   * If crawling is allowed, the [[PageCandidate]] is sent to a [[PageManager]] so that it can be discovered.
   */
  private def filterPageCandidate(candidate: PageCandidate, hostInfo: HostInformation): Unit = {
    if (hostInfo.robotRules.isAllowed(candidate.url)) {
      val pageManager = sharding.entityRefFor(PageManager.TypeKey, candidate.url)
      pageManager ! PageManager.Discover(candidate.crawlDepth)
      metrics.addFilterPassed(1)
    } else {
      metrics.addFilterRejected(1)
    }
  }
}
