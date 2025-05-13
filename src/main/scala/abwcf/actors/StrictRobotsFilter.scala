package abwcf.actors

import abwcf.data.{HostInformation, Page, PageStatus}
import abwcf.util.UrlUtils
import com.github.benmanes.caffeine.cache.{Caffeine, Expiry}
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding
import org.apache.pekko.util.Timeout

import java.time.Instant
import scala.collection.mutable
import scala.jdk.DurationConverters.*
import scala.util.{Failure, Success}

/**
 * Filters out [[Page]]s that should not be crawled based on the Robots Exclusion Protocol.
 *
 * [[Page]]s are not sent downstream if the required [[HostInformation]] is currently unavailable (e.g. if the corresponding `robots.txt` file has not been fetched yet).
 *
 * There should be one [[StrictRobotsFilter]] actor per node.
 *
 * This actor is stateful.
 *
 * @see
 *      - [[https://datatracker.ietf.org/doc/html/rfc9309 RFC 9309 - Robots Exclusion Protocol]]
 *      - [[https://en.wikipedia.org/wiki/Robots.txt Wikipedia: robots.txt]]
 */
object StrictRobotsFilter {
  sealed trait Command
  case class Filter(page: Page) extends Command
  private case class AskFailure(schemeAndAuthority: String) extends Command

  private type CombinedCommand = Command | HostManager.HostInfo

  def apply(): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    new StrictRobotsFilter(context).strictRobotsFilter()
  }).narrow
}

private class StrictRobotsFilter private (context: ActorContext[StrictRobotsFilter.CombinedCommand]) {
  import StrictRobotsFilter.*

  private val sharding = ClusterSharding(context.system)
  private val config = context.system.settings.config
  private val askTimeout = config.getDuration("abwcf.actors.strict-robots-filter.ask-timeout").toScala
  private val failCloseDuration = config.getDuration("abwcf.actors.strict-robots-filter.fail-close-duration")
  private val maxCacheSize = config.getLong("abwcf.actors.strict-robots-filter.max-cache-size")

  /**
   * '''Key:''' Scheme and authority of the [[HostInformation]].<br>
   * '''Value:''' `None` if the [[HostInformation]] is marked as unavailable, otherwise `Some[HostInformation]`.
   */
  private val hostInfoCache = Caffeine.newBuilder() //Mutable state!
    .maximumSize(maxCacheSize)
    .expireAfter(Expiry.writing[String, Option[HostInformation]]({
      case (_, Some(hostInfo)) => Instant.now.until(hostInfo.validUntil)
      case (_, None) => failCloseDuration
    }))
    .build[String, Option[HostInformation]]()

  /**
   * Buffers [[Page]]s while the request for the required [[HostInformation]] is in progress.
   */
  private val pendingPages = mutable.HashMap.empty[String, mutable.ArrayBuffer[Page]] //Mutable state!

  private def strictRobotsFilter(): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case Filter(page) =>
      val schemeAndAuthority = UrlUtils.getSchemeAndAuthority(page.url)
      val cachedValue = hostInfoCache.getIfPresent(schemeAndAuthority)

      cachedValue match {
        case Some(hostInfo) => filterPage(page, hostInfo) //Cache hit
        case None => () //Cache hit, but the HostInformation is marked as unavailable. The page is ignored.

        case null => //Cache miss
          //Buffer the page:
          val pages = pendingPages.getOrElse(schemeAndAuthority, mutable.ArrayBuffer.empty)
          pendingPages.update(schemeAndAuthority, pages.append(page))

          //Request information from the HostManager:
          if (pages.size == 1) { //If the buffer contains more than one element at this point, then a request is already in progress.
            val hostManager = sharding.entityRefFor(HostManager.TypeKey, schemeAndAuthority)

            context.ask(hostManager, HostManager.GetHostInfo.apply)({
              case Success(reply) => reply
              case Failure(_) => AskFailure(schemeAndAuthority)
            })(using askTimeout)
          }
      }

      Behaviors.same

    case HostManager.HostInfo(hostInfo) =>
      hostInfoCache.put(hostInfo.schemeAndAuthority, Some(hostInfo))

      //Filter the buffered pages:
      pendingPages.remove(hostInfo.schemeAndAuthority)
        .getOrElse(mutable.ArrayBuffer.empty)
        .foreach(page => filterPage(page, hostInfo))

      Behaviors.same

    case AskFailure(schemeAndAuthority) =>
      hostInfoCache.put(schemeAndAuthority, None) //Mark the HostInformation as unavailable.
      pendingPages.remove(schemeAndAuthority) //Drop the buffered pages.
      Behaviors.same
  })

  /**
   * Checks if the [[Page]] is allowed to be crawled based on the Robots Exclusion Protocol.
   *
   * If crawling is allowed, the [[Page]] is added to a [[HostQueue]] so that it can be fetched.
   *
   * If crawling is disallowed, the corresponding [[PageManager]] is notified so that it can update the status of the page.
   */
  private def filterPage(page: Page, hostInfo: HostInformation): Unit = {
    if (hostInfo.robotRules.isAllowed(page.url)) {
      val hostQueue = sharding.entityRefFor(HostQueue.TypeKey, UrlUtils.getSchemeAndAuthority(page.url))
      hostQueue ! HostQueue.Enqueue(page)
    } else {
      val pageManager = sharding.entityRefFor(PageManager.TypeKey, page.url)
      pageManager ! PageManager.SetStatus(PageStatus.Disallowed)
    }
  }
}
