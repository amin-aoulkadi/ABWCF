package abwcf.actors

import abwcf.actors.persistence.page.PagePersistence
import abwcf.data.{Page, PageCandidate, PageStatus}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import org.apache.pekko.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}

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
  case class SetStatus(status: PageStatus) extends Command
  private case object Passivate extends Command

  sealed trait PersistenceCommand extends Command //These have to be part of the public protocol so that they work with ShardingEnvelopes.
  case class RecoveryResult(result: Option[Page]) extends PersistenceCommand
  case object InsertSuccess extends PersistenceCommand
  case object UpdateSuccess extends PersistenceCommand

  def apply(entityContext: EntityContext[Command],
            pagePersistenceManager: ActorRef[PagePersistence.Command],
            prioritizer: ActorRef[Prioritizer.Command],
            strictRobotsFilter: ActorRef[StrictRobotsFilter.Command]): Behavior[Command] =
    Behaviors.setup(context => {
      Behaviors.withStash(100)(buffer => {
        new PageManager(pagePersistenceManager, prioritizer, strictRobotsFilter, entityContext.shard, context, buffer).recovering(entityContext.entityId)
      })
  })

  def initializeSharding(system: ActorSystem[?],
                         pagePersistenceManager: ActorRef[PagePersistence.Command],
                         prioritizer: ActorRef[Prioritizer.Command],
                         strictRobotsFilter: ActorRef[StrictRobotsFilter.Command]): ActorRef[ShardingEnvelope[Command]] = {
    val settings = ClusterShardingSettings(system)
      .withRememberEntities(false) //Pages are periodically restored by the PageRestorer, so it doesn't make sense to remember them.
      .withNoPassivationStrategy() //Disable automatic passivation.

    ClusterSharding(system).init(
      Entity(TypeKey)(entityContext => PageManager(entityContext, pagePersistenceManager, prioritizer, strictRobotsFilter))
        .withSettings(settings)
    )
  }
}

private class PageManager private(pagePersistenceManager: ActorRef[PagePersistence.Command],
                                  prioritizer: ActorRef[Prioritizer.Command],
                                  strictRobotsFilter: ActorRef[StrictRobotsFilter.Command],
                                  shard: ActorRef[ClusterSharding.ShardCommand],
                                  context: ActorContext[PageManager.Command],
                                  buffer: StashBuffer[PageManager.Command]) {
  import PageManager.*

  private val sharding = ClusterSharding(context.system)
  private val config = context.system.settings.config
  private val receiveTimeout = config.getDuration("abwcf.actors.page-manager.passivation-receive-timeout").toScala

  private def recovering(url: String): Behavior[Command] = {
    pagePersistenceManager ! PagePersistence.Recover(url) //TODO: Inefficient if the RecoveryResult comes from the PageRestorer.

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
    prioritizer ! Prioritizer.Prioritize(candidate)

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
    pagePersistenceManager ! PagePersistence.Insert(page)

    Behaviors.receiveMessage({
      case Discover(_) => Behaviors.same //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.
      case InsertSuccess => buffer.unstashAll(discoveredPage(page))

      case other =>
        buffer.stash(other)
        Behaviors.same
    })
  }

  private def discoveredPage(page: Page): Behavior[Command] = {
    strictRobotsFilter ! StrictRobotsFilter.Filter(page) //Send the page downstream so that it can be fetched.
    context.setReceiveTimeout(receiveTimeout, Passivate) //Enable passivation.

    Behaviors.receiveMessage({
      case SetStatus(status) =>
        pagePersistenceManager ! PagePersistence.UpdateStatus(page.url, status)
        updating(page.copy(status = status))

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
