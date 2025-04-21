package abwcf.actors

import abwcf.actors.persistence.host.HostPersistence
import abwcf.data.HostInformation
import abwcf.util.ActorRegistry
import crawlercommons.robots.{SimpleRobotRules, SimpleRobotRulesParser}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import org.apache.pekko.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}

import java.time.{Duration, Instant}
import java.util.Collections

/**
 * Represents a host and the associated [[HostInformation]].
 *
 * There should be exactly one [[HostManager]] actor per [[HostInformation]].
 *
 * This actor is stateful, sharded, gracefully passivated and persisted.
 *
 * Entity ID: Scheme and authority components of a URL.
 *
 * This entity will not be remembered (even if remembering entities is enabled).
 */
object HostManager {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("HostManager")

  sealed trait Command
  case class GetHostInfo(replyTo: ActorRef[HostInfo]) extends Command

  sealed trait PersistenceCommand extends Command //These have to be part of the public protocol so that they work with ShardingEnvelopes.
  case class RecoveryResult(result: Option[HostInformation]) extends PersistenceCommand
  case object InsertSuccess extends PersistenceCommand
  case object UpdateSuccess extends PersistenceCommand

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

private class HostManager private (context: ActorContext[HostManager.CombinedCommand],
                                   buffer: StashBuffer[HostManager.CombinedCommand]) {
  import HostManager.*
  
  private val hostPersistenceManager = ActorRegistry.hostPersistenceManager.get
  //TODO: Passivation.

  private def recovering(schemeAndAuthority: String): Behavior[CombinedCommand] = {
    hostPersistenceManager ! HostPersistence.Recover(schemeAndAuthority)

    Behaviors.receiveMessage({
      case RecoveryResult(None) =>
        fetching(schemeAndAuthority, None)

      case RecoveryResult(Some(hostInfo)) if hostInfo.isExpired =>
        fetching(schemeAndAuthority, Some(hostInfo)) //Fetch the robots.txt file again.

      case RecoveryResult(Some(hostInfo)) =>
        buffer.unstashAll(knownHost(hostInfo))

      case other =>
        buffer.stash(other)
        Behaviors.same
    })
  }

  private def fetching(schemeAndAuthority: String, expiredHostInfo: Option[HostInformation]): Behavior[CombinedCommand] = {
    val robotsFetcher = context.spawnAnonymous(RobotsFetcher(schemeAndAuthority, context.self))
    val parser = new SimpleRobotRulesParser(Long.MaxValue, 3) //TODO: Handle max crawl delay.

    def nextBehavior(hostInfo: HostInformation) = expiredHostInfo match {
      case Some(_) => persisting(hostInfo, HostPersistence.Update(hostInfo)) //There is already an entry in the database, so it needs to be updated.
      case None => persisting(hostInfo, HostPersistence.Insert(hostInfo)) //There is no database entry yet.
    }

    Behaviors.receiveMessage({
      case RobotsFetcher.Response(responseBody) =>
        val rules = parser.parseContent(schemeAndAuthority, responseBody.toArray, "text/plain", Collections.emptyList()) //The RobotsFetcher only fetches "text/plain". //TODO: User-agent name.
        rules.sortRules()
        context.log.info("Processed robots.txt for {} and found {} rules", schemeAndAuthority, rules.getRobotRules.size)
        val hostInfo = HostInformation(schemeAndAuthority, rules, Instant.now.plus(Duration.ofHours(24)))
        nextBehavior(hostInfo)

      case RobotsFetcher.Unavailable =>
        context.log.info("The robots.txt for {} is unavailable, all resources on this host can be crawled", schemeAndAuthority)
        val rules = SimpleRobotRules(SimpleRobotRules.RobotRulesMode.ALLOW_ALL) //If the robots.txt file is unavailable, everything is allowed.
        val hostInfo = HostInformation(schemeAndAuthority, rules, Instant.now.plus(Duration.ofHours(24)))
        nextBehavior(hostInfo)

      case RobotsFetcher.Unreachable =>
        expiredHostInfo match {
          case Some(oldHostInfo) =>
            context.log.info("The robots.txt for {} is unreachable, expired rules for this host will be reused", schemeAndAuthority)
            val hostInfo = oldHostInfo.copy(validUntil = Instant.now.plus(Duration.ofHours(1)))
            persisting(hostInfo, HostPersistence.Update(hostInfo))

          case None =>
            context.log.info("The robots.txt for {} is unreachable, this host will not be crawled", schemeAndAuthority)
            val rules = SimpleRobotRules(SimpleRobotRules.RobotRulesMode.ALLOW_NONE) //If the robots.txt file is unreachable, nothing is allowed.
            val hostInfo = HostInformation(schemeAndAuthority, rules, Instant.now.plus(Duration.ofHours(1)))
            persisting(hostInfo, HostPersistence.Insert(hostInfo))
        }

      case other =>
        buffer.stash(other)
        Behaviors.same
    })
  }

  private def persisting(hostInfo: HostInformation, persistenceCommand: HostPersistence.Insert | HostPersistence.Update): Behavior[CombinedCommand] = {
    hostPersistenceManager ! persistenceCommand

    Behaviors.receiveMessage({
      case InsertSuccess | UpdateSuccess =>
        buffer.unstashAll(knownHost(hostInfo))

      case other =>
        buffer.stash(other)
        Behaviors.same
    })
  }

  private def knownHost(hostInfo: HostInformation): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case request: GetHostInfo if hostInfo.isExpired =>
      buffer.stash(request)
      fetching(hostInfo.schemeAndAuthority, Some(hostInfo)) //Fetch the robots.txt file again.

    case GetHostInfo(replyTo) =>
      replyTo ! HostInfo(hostInfo)
      Behaviors.same

    case _ => Behaviors.same
  })
}
