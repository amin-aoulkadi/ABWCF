package abwcf.actors

import abwcf.data.HostInformation
import crawlercommons.robots.{SimpleRobotRules, SimpleRobotRulesParser}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import org.apache.pekko.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}

import java.time.{Duration, Instant}
import java.util.Collections

object HostManager {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("HostManager")

  sealed trait Command
  case class GetHostInfo(replyTo: ActorRef[HostInfo]) extends Command

  sealed trait Reply
  case class HostInfo(hostInfo: HostInformation) extends Reply

  private type CombinedCommand = Command | RobotsFetcher.Reply

  def apply(entityContext: EntityContext[Command]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    Behaviors.withStash(100)(buffer => { //TODO: The stash can fill up with GetHostInfo messages.
      new HostManager(context, buffer).recovering(entityContext.entityId)
    })
  }).narrow

  def getShardRegion(system: ActorSystem[?]): ActorRef[ShardingEnvelope[Command]] = {
    val settings = ClusterShardingSettings(system)
      .withRememberEntities(false) //There is no need to remember HostManagers.
      .withNoPassivationStrategy() //Disable automatic passivation.

    ClusterSharding(system).init(
      Entity(TypeKey)(entityContext => HostManager(entityContext))
        .withSettings(settings)
    )
  }
}

private class HostManager private (context: ActorContext[HostManager.CombinedCommand], buffer: StashBuffer[HostManager.CombinedCommand]) {
  import HostManager.*
  //TODO: Passivation.

  private def recovering(schemeAndAuthority: String): Behavior[CombinedCommand] = {
    //TODO: Recover.
    //TODO: Check HostInformation expiry after successful recovery.
    fetching(schemeAndAuthority)
  }

  private def fetching(schemeAndAuthority: String): Behavior[CombinedCommand] = {
    val robotsFetcher = context.spawnAnonymous(RobotsFetcher(schemeAndAuthority, context.self))
    val parser = new SimpleRobotRulesParser(Long.MaxValue, 3) //TODO: Handle max crawl delay.

    Behaviors.receiveMessage({
      case RobotsFetcher.Response(responseBody) =>
        val rules = parser.parseContent(schemeAndAuthority, responseBody.toArray, "text/plain", Collections.emptyList()) //The RobotsFetcher only fetches "text/plain". //TODO: User-agent name.
        rules.sortRules()
        context.log.info("Processed robots.txt for {} and found {} rules", schemeAndAuthority, rules.getRobotRules.size)
        val hostInfo = HostInformation(schemeAndAuthority, rules, Instant.now.plus(Duration.ofHours(24)))
        buffer.unstashAll(persisting(hostInfo))

      case RobotsFetcher.Unavailable =>
        context.log.info("The robots.txt for {} is unavailable, all resources on this host can be crawled", schemeAndAuthority)
        val rules = SimpleRobotRules(SimpleRobotRules.RobotRulesMode.ALLOW_ALL) //If the robots.txt file is unavailable, everything is allowed.
        val hostInfo = HostInformation(schemeAndAuthority, rules, Instant.now.plus(Duration.ofHours(24)))
        buffer.unstashAll(persisting(hostInfo))

      case RobotsFetcher.Unreachable =>
        context.log.info("The robots.txt for {} is unreachable, this host will not be crawled", schemeAndAuthority)
        val rules = SimpleRobotRules(SimpleRobotRules.RobotRulesMode.ALLOW_NONE) //If the robots.txt file is unreachable, nothing is allowed.
        val hostInfo = HostInformation(schemeAndAuthority, rules, Instant.now.plus(Duration.ofHours(1)))
        buffer.unstashAll(persisting(hostInfo))

      case other =>
        buffer.stash(other)
        Behaviors.same
    })
  }

  private def persisting(hostInfo: HostInformation): Behavior[CombinedCommand] = {
    //TODO: Persist.
    knownHost(hostInfo)
  }

  private def knownHost(hostInfo: HostInformation): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case GetHostInfo(replyTo) if Instant.now.isBefore(hostInfo.validUntil) =>
      replyTo ! HostInfo(hostInfo)
      Behaviors.same

    case request: GetHostInfo => //The HostInformation has expired.
      buffer.stash(request)
      fetching(hostInfo.schemeAndAuthority)

    case _ => Behaviors.same
  })
}
