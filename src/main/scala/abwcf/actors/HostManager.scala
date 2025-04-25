package abwcf.actors

import abwcf.actors.persistence.host.HostPersistence
import abwcf.data.HostInformation
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode
import crawlercommons.robots.{BaseRobotRules, SimpleRobotRules, SimpleRobotRulesParser}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import org.apache.pekko.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}

import java.time.Instant
import java.util.Locale
import java.util.stream.Collectors
import scala.jdk.DurationConverters.*

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
  private case object Passivate extends Command

  sealed trait PersistenceCommand extends Command //These have to be part of the public protocol so that they work with ShardingEnvelopes.
  case class RecoveryResult(result: Option[HostInformation]) extends PersistenceCommand
  case object InsertSuccess extends PersistenceCommand
  case object UpdateSuccess extends PersistenceCommand

  sealed trait Reply
  case class HostInfo(hostInfo: HostInformation) extends Reply

  private type CombinedCommand = Command | RobotsFetcher.Reply

  def apply(entityContext: EntityContext[Command], hostPersistenceManager: ActorRef[HostPersistence.Command]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    Behaviors.withStash(100)(buffer => { //TODO: The stash can fill up with GetHostInfo messages.
      new HostManager(hostPersistenceManager, entityContext.shard, context, buffer).recovering(entityContext.entityId)
    })
  }).narrow

  def initializeSharding(system: ActorSystem[?], hostPersistenceManager: ActorRef[HostPersistence.Command]): ActorRef[ShardingEnvelope[Command]] = {
    val settings = ClusterShardingSettings(system)
      .withRememberEntities(false) //There is no need to remember HostManagers.
      .withNoPassivationStrategy() //Disable automatic passivation.

    ClusterSharding(system).init(
      Entity(TypeKey)(entityContext => HostManager(entityContext, hostPersistenceManager))
        .withSettings(settings)
    )
  }
}

private class HostManager private (hostPersistenceManager: ActorRef[HostPersistence.Command],
                                   shard: ActorRef[ClusterSharding.ShardCommand],
                                   context: ActorContext[HostManager.CombinedCommand],
                                   buffer: StashBuffer[HostManager.CombinedCommand]) {
  import HostManager.*

  private val config = context.system.settings.config
  private val userAgents = config.getStringList("abwcf.robots.user-agents").stream()
    .map(_.toLowerCase(Locale.ROOT)) //The SimpleRobotRulesParser expects product tokens in lowercase.
    .collect(Collectors.toList)
  private val defaultCrawlDelay = config.getDuration("abwcf.robots.default-crawl-delay").toMillis
  private val minCrawlDelay = config.getDuration("abwcf.robots.min-crawl-delay").toMillis
  private val maxCrawlDelay = config.getDuration("abwcf.robots.max-crawl-delay").toMillis
  private val validRulesLifetime = config.getDuration("abwcf.robots.valid-rules-lifetime")
  private val unavailableRulesLifetime = config.getDuration("abwcf.robots.unavailable-rules-lifetime")
  private val unreachableRulesLifetime = config.getDuration("abwcf.robots.unreachable-rules-lifetime")
  private val receiveTimeout = config.getDuration("abwcf.actors.host-manager.passivation-receive-timeout").toScala

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
    val parser = new SimpleRobotRulesParser(Long.MaxValue, 3) //Disable the built-in maximum crawl delay logic of the SimpleRobotRulesParser.

    def nextBehavior(hostInfo: HostInformation) = expiredHostInfo match {
      case Some(_) => persisting(hostInfo, HostPersistence.Update(hostInfo)) //There is already an entry in the database, so it needs to be updated.
      case None => persisting(hostInfo, HostPersistence.Insert(hostInfo)) //There is no database entry yet.
    }

    Behaviors.receiveMessage({
      case RobotsFetcher.Response(responseBody) =>
        //Parse the robots.txt file:
        val rules = parser.parseContent(schemeAndAuthority, responseBody.toArray, "text/plain", userAgents) //The RobotsFetcher only fetches "text/plain".
        rules.sortRules()

        //Determine the crawl delay:
        var crawlDelay = rules.getCrawlDelay match {
          case BaseRobotRules.UNSET_CRAWL_DELAY => defaultCrawlDelay //Use the default crawl delay if none is specified in the robots.txt file.
          case delay if delay < 0 => defaultCrawlDelay
          case delay => delay
        }

        crawlDelay = crawlDelay
          .max(minCrawlDelay) //Ensure that crawlDelay ≥ minCrawlDelay.
          .min(maxCrawlDelay) //Ensure that crawlDelay ≤ maxCrawlDelay.

        rules.setCrawlDelay(crawlDelay)

        context.log.info("Processed robots.txt for {}, found {} rules and set the crawl delay to {} ms", schemeAndAuthority, rules.getRobotRules.size, crawlDelay)
        val hostInfo = HostInformation(schemeAndAuthority, rules, Instant.now.plus(validRulesLifetime))
        nextBehavior(hostInfo)

      case RobotsFetcher.Unavailable =>
        context.log.info("The robots.txt for {} is unavailable, all resources on this host can be crawled", schemeAndAuthority)
        val rules = new SimpleRobotRules(RobotRulesMode.ALLOW_ALL) //If the robots.txt file is unavailable, everything is allowed.
        rules.setCrawlDelay(defaultCrawlDelay)
        val hostInfo = HostInformation(schemeAndAuthority, rules, Instant.now.plus(unavailableRulesLifetime))
        nextBehavior(hostInfo)

      case RobotsFetcher.Unreachable =>
        expiredHostInfo match {
          case Some(oldHostInfo) =>
            context.log.info("The robots.txt for {} is unreachable, expired rules for this host will be reused", schemeAndAuthority)
            val hostInfo = oldHostInfo.copy(validUntil = Instant.now.plus(unreachableRulesLifetime))
            persisting(hostInfo, HostPersistence.Update(hostInfo))

          case None =>
            context.log.info("The robots.txt for {} is unreachable, this host will not be crawled", schemeAndAuthority)
            val rules = new SimpleRobotRules(RobotRulesMode.ALLOW_NONE) //If the robots.txt file is unreachable, nothing is allowed.
            rules.setCrawlDelay(defaultCrawlDelay)
            val hostInfo = HostInformation(schemeAndAuthority, rules, Instant.now.plus(unreachableRulesLifetime))
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

  private def knownHost(hostInfo: HostInformation): Behavior[CombinedCommand] = {
    context.setReceiveTimeout(receiveTimeout, Passivate) //Enable passivation.

    Behaviors.receiveMessage({
      case request: GetHostInfo if hostInfo.isExpired =>
        buffer.stash(request)
        fetching(hostInfo.schemeAndAuthority, Some(hostInfo)) //Fetch the robots.txt file again.

      case GetHostInfo(replyTo) =>
        replyTo ! HostInfo(hostInfo)
        Behaviors.same

      case Passivate =>
        shard ! ClusterSharding.Passivate(context.self)
        Behaviors.same

      case _ => Behaviors.same
    })
  }
}
