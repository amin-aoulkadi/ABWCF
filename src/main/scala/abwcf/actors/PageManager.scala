package abwcf.actors

import abwcf.actors.persistence.PagePersistence
import abwcf.data.{Page, PageCandidate, PageStatus}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import org.apache.pekko.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}

import java.net.URI
import scala.jdk.DurationConverters.*

/**
 * Represents a page to be crawled.
 *
 * There should be exactly one [[PageManager]] actor per page.
 *
 * This actor is stateful, sharded, gracefully passivated and persisted.
 *
 * Entity ID: URL of the page.
 *
 * This entity will not be remembered (even if remembering entities is enabled).
 */
object PageManager {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("PageManager")

  sealed trait Command
  case class Discover(crawlDepth: Int) extends Command
  case class SetPriority(priority: Long) extends Command
  case object Success extends Command
  case object Redirect extends Command
  case object Error extends Command
  private case object Passivate extends Command

  sealed trait PersistenceCommand extends Command //These have to be part of the public protocol so that they work with ShardingEnvelopes.
  case class RecoveryResult(result: Option[Page]) extends PersistenceCommand
  case object InsertSuccess extends PersistenceCommand
  case object UpdateSuccess extends PersistenceCommand

  def apply(entityContext: EntityContext[Command],
            hostQueueShardRegion: ActorRef[ShardingEnvelope[HostQueue.Command]],
            pageManager: ActorRef[PageGateway.CombinedCommand]): Behavior[Command] =
    Behaviors.setup(context => {
      Behaviors.withStash(100)(buffer => {
        new PageManager(hostQueueShardRegion, pageManager, entityContext.shard, context, buffer).recovering(entityContext.entityId)
      })
  })

  def getShardRegion(system: ActorSystem[?], pageManager: ActorRef[PageGateway.CombinedCommand]): ActorRef[ShardingEnvelope[Command]] = {
    val hostQueueShardRegion = HostQueue.getShardRegion(system) //Getting the shard region here (instead of in Page.apply()) significantly reduces spam in the log.
    val settings = ClusterShardingSettings(system)
      .withRememberEntities(false) //Pages are periodically restored by the PageRestorer, so it doesn't make sense to remember them.
      .withNoPassivationStrategy() //Disable automatic passivation.

    ClusterSharding(system).init(
      Entity(TypeKey)(entityContext => PageManager(entityContext, hostQueueShardRegion, pageManager))
        .withSettings(settings)
    )
  }
}

private class PageManager private(hostQueueShardRegion: ActorRef[ShardingEnvelope[HostQueue.Command]],
                                  pageManager: ActorRef[PageGateway.CombinedCommand],
                                  shard: ActorRef[ClusterSharding.ShardCommand],
                                  context: ActorContext[PageManager.Command],
                                  buffer: StashBuffer[PageManager.Command]) {
  import PageManager.*

  private val config = context.system.settings.config
  private val receiveTimeout = config.getDuration("abwcf.page.passivation-receive-timeout").toScala

  /**
   * Adds the URL to a [[HostQueue]] so that it can be fetched.
   */
  private def addToHostQueue(page: Page): Unit = {
    val host = URI(page.url).getHost
    hostQueueShardRegion ! ShardingEnvelope(host, HostQueue.Enqueue(page))
  }

  private def recovering(url: String): Behavior[Command] = {
    pageManager ! PagePersistence.Recover(url)

    Behaviors.receiveMessage({
      case RecoveryResult(None) =>
        buffer.unstashAll(newPage(url))

      case RecoveryResult(Some(page)) if page.status == PageStatus.Discovered =>
        buffer.unstashAll(discoveredPage(page))

      case RecoveryResult(Some(page)) if page.status == PageStatus.Processed =>
        buffer.unstashAll(processedPage(page))

      case other =>
        buffer.stash(other)
        Behaviors.same
    })
  }

  private def newPage(url: String): Behavior[Command] = Behaviors.receiveMessage({
    case Discover(crawlDepth) =>
      val candidate = PageCandidate(url, crawlDepth)
      buffer.unstashAll(prioritizing(candidate))

    case other =>
      buffer.stash(other)
      Behaviors.same
  })

  private def prioritizing(candidate: PageCandidate): Behavior[Command] = {
    pageManager ! Prioritizer.Prioritize(candidate)

    Behaviors.receiveMessage({
      case Discover(_) => Behaviors.same //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.

      case SetPriority(priority) =>
        val page = Page(candidate.url, PageStatus.Discovered, candidate.crawlDepth, priority)
        buffer.unstashAll(inserting(page))

      case other =>
        buffer.stash(other)
        Behaviors.same
    })
  }

  private def inserting(page: Page): Behavior[Command] = {
    pageManager ! PagePersistence.Insert(page)

    Behaviors.receiveMessage({
      case Discover(_) => Behaviors.same //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.
      case InsertSuccess => buffer.unstashAll(discoveredPage(page))

      case other =>
        buffer.stash(other)
        Behaviors.same
    })
  }

  private def discoveredPage(page: Page): Behavior[Command] = {
    addToHostQueue(page)
    context.setReceiveTimeout(receiveTimeout, Passivate) //Enable passivation.

    Behaviors.receiveMessage({
      case Success | Redirect | Error =>
        pageManager ! PagePersistence.UpdateStatus(page.url, PageStatus.Processed)
        updating(page.copy(status = PageStatus.Processed))

      case Passivate =>
        shard ! ClusterSharding.Passivate(context.self)
        Behaviors.same

      case _ => Behaviors.same
    })
  }

  private def updating(page: Page): Behavior[Command] = Behaviors.receiveMessage({
    case Discover(_) => Behaviors.same //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.
    case UpdateSuccess => buffer.unstashAll(processedPage(page))

    case other =>
      buffer.stash(other)
      Behaviors.same
  })

  private def processedPage(page: Page): Behavior[Command] = {
    context.setReceiveTimeout(receiveTimeout, Passivate) //Enable passivation.

    Behaviors.receiveMessage({
      case Passivate =>
        shard ! ClusterSharding.Passivate(context.self)
        Behaviors.same

      case _ => Behaviors.same
    })
  }
}
