package abwcf.actors

import abwcf.actors.persistence.PagePersistenceManager
import abwcf.{PageEntity, PageStatus}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import org.apache.pekko.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}

import java.net.URI
import scala.jdk.DurationConverters.*

/**
 * Represents a page to be crawled.
 *
 * There should be exactly one [[Page]] actor per page. [[Page]] actors should be managed by a [[PageManager]] actor.
 *
 * This actor is stateful, sharded, gracefully passivated and persisted.
 *
 * Entity ID: URL of the page.
 *
 * This entity will not be remembered (even if remembering entities is enabled).
 */
object Page {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("Page")

  sealed trait Command
  case class Discover(crawlDepth: Int) extends Command
  case object Success extends Command
  case object Redirect extends Command
  case object Error extends Command
  private case object Passivate extends Command

  sealed trait PersistenceCommand extends Command //These have to be part of the public protocol so that they work with ShardingEnvelopes.
  case class RecoveryResult(result: Option[PageEntity]) extends PersistenceCommand
  case object InsertSuccess extends PersistenceCommand
  case object UpdateSuccess extends PersistenceCommand

  def apply(entityContext: EntityContext[Command],
            hostQueueShardRegion: ActorRef[ShardingEnvelope[HostQueue.Command]],
            persistence: ActorRef[PagePersistenceManager.Command]): Behavior[Command] =
    Behaviors.setup(context => {
      Behaviors.withStash(100)(buffer => {
        new Page(hostQueueShardRegion, persistence, entityContext.shard, context, buffer).recovering(entityContext.entityId)
      })
  })

  def getShardRegion(system: ActorSystem[?], pagePersistenceManager: ActorRef[PagePersistenceManager.Command]): ActorRef[ShardingEnvelope[Command]] = {
    val hostQueueShardRegion = HostQueue.getShardRegion(system) //Getting the shard region here (instead of in Page.apply()) significantly reduces spam in the log.
    val settings = ClusterShardingSettings(system)
      .withRememberEntities(false) //Pages are periodically restored by the PageRestorer, so it doesn't make sense to remember them.
      .withNoPassivationStrategy() //Disable automatic passivation.

    ClusterSharding(system).init(
      Entity(TypeKey)(entityContext => Page(entityContext, hostQueueShardRegion, pagePersistenceManager))
        .withSettings(settings)
    )
  }
}

private class Page private (hostQueueShardRegion: ActorRef[ShardingEnvelope[HostQueue.Command]],
                            persistence: ActorRef[PagePersistenceManager.Command],
                            shard: ActorRef[ClusterSharding.ShardCommand],
                            context: ActorContext[Page.Command],
                            buffer: StashBuffer[Page.Command]) {
  import Page.*

  private val config = context.system.settings.config
  private val receiveTimeout = config.getDuration("abwcf.page.passivation-receive-timeout").toScala

  /**
   * Adds the URL to a [[HostQueue]] so that it can be fetched.
   */
  private def addToHostQueue(page: PageEntity): Unit = {
    val host = URI(page.url).getHost
    hostQueueShardRegion ! ShardingEnvelope(host, HostQueue.Enqueue(page))
  }

  private def recovering(url: String): Behavior[Command] = {
    persistence ! PagePersistenceManager.Recover(url)

    Behaviors.receiveMessage({
      case RecoveryResult(Some(page)) if page.status == PageStatus.Discovered =>
        buffer.unstashAll(discoveredPage(page))

      case RecoveryResult(Some(page)) if page.status == PageStatus.Processed =>
        buffer.unstashAll(processedPage(page))

      case RecoveryResult(None) =>
        buffer.unstashAll(unknownPage(url))

      case other =>
        buffer.stash(other)
        Behaviors.same
    })
  }

  private def unknownPage(url: String): Behavior[Command] = Behaviors.receiveMessage({
    case Discover(crawlDepth) =>
      val page = PageEntity(url, PageStatus.Discovered, crawlDepth)
      persistence ! PagePersistenceManager.Insert(page)
      buffer.unstashAll(inserting(page))

    case other =>
      buffer.stash(other)
      Behaviors.same
  })

  private def inserting(page: PageEntity): Behavior[Command] = Behaviors.receiveMessage({
    case Discover(_) => Behaviors.same //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.
    case InsertSuccess => buffer.unstashAll(discoveredPage(page))

    case other =>
      buffer.stash(other)
      Behaviors.same
  })

  private def discoveredPage(page: PageEntity): Behavior[Command] = {
    addToHostQueue(page)
    context.setReceiveTimeout(receiveTimeout, Passivate) //Enable passivation.

    Behaviors.receiveMessage({
      case _: PersistenceCommand => Behaviors.same //The PageRestorer may attempt to restore pages that are already active.
      case Discover(_) => Behaviors.same //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.

      case Success | Redirect | Error =>
        persistence ! PagePersistenceManager.UpdateStatus(page.url, PageStatus.Processed)
        updating(page.copy(status = PageStatus.Processed))

      case Passivate =>
        shard ! ClusterSharding.Passivate(context.self)
        Behaviors.same
    })
  }

  private def updating(page: PageEntity): Behavior[Command] = Behaviors.receiveMessage({
    case Discover(_) => Behaviors.same //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.
    case UpdateSuccess => buffer.unstashAll(processedPage(page))

    case other =>
      buffer.stash(other)
      Behaviors.same
  })

  private def processedPage(page: PageEntity): Behavior[Command] = {
    context.setReceiveTimeout(receiveTimeout, Passivate) //Enable passivation.

    Behaviors.receiveMessage({
      case Passivate =>
        shard ! ClusterSharding.Passivate(context.self)
        Behaviors.same

      case _ => Behaviors.same
    })
  }
}
