package abwcf.actors

import abwcf.data.{HostInformation, PageCandidate}
import abwcf.util.UrlUtils
import com.github.benmanes.caffeine.cache.{Caffeine, Expiry}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

import java.time.Instant
import scala.collection.mutable

//TODO: Documentation.
object RobotsFilter {
  sealed trait Command
  case class Filter(candidate: PageCandidate) extends Command

  private type CombinedCommand = Command | HostManager.HostInfo

  def apply(hostGateway: ActorRef[HostGateway.CombinedCommand], pageGateway: ActorRef[PageGateway.Command]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    val hostShardRegion = HostManager.getShardRegion(context.system, hostGateway)

    val cache = Caffeine.newBuilder() //Mutable state!
      .maximumSize(1000) //TODO: Add to config.
      .expireAfter(Expiry.writing[String, HostInformation]((_, hostInfo) => Instant.now.until(hostInfo.validUntil)))
      .build[String, HostInformation]()

    val pendingCandidates = mutable.HashMap.empty[String, mutable.ArrayBuffer[PageCandidate]] //Mutable state!

    Behaviors.receiveMessage({
      case Filter(candidate) =>
        val schemeAndAuthority = UrlUtils.getSchemeAndAuthority(candidate.url)
        val hostInfo = cache.getIfPresent(schemeAndAuthority)

        if (hostInfo == null) {
          //Buffer the URL and request information from the HostManager:
          val candidates = pendingCandidates.getOrElse(schemeAndAuthority, mutable.ArrayBuffer.empty)
          pendingCandidates.update(schemeAndAuthority, candidates.append(candidate))
          hostShardRegion ! ShardingEnvelope(schemeAndAuthority, HostManager.GetHostInfo(context.self))
        } else {
          //Filter the URL:
          if (hostInfo.robotRules.isAllowed(candidate.url)) {
            pageGateway ! PageGateway.Discover(candidate)
          }
        }

        Behaviors.same

      case HostManager.HostInfo(hostInfo) =>
        cache.put(hostInfo.schemeAndAuthority, hostInfo)

        //Filter the buffered URLs:
        pendingCandidates.remove(hostInfo.schemeAndAuthority)
          .getOrElse(mutable.ArrayBuffer.empty)
          .filter(candidate => hostInfo.robotRules.isAllowed(candidate.url))
          .foreach(candidate => pageGateway ! PageGateway.Discover(candidate))

        Behaviors.same
    })
  }).narrow
}
