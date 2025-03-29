package abwcf.actors

import abwcf.actors.persistence.PagePersistence
import abwcf.{PageEntity, PageStatus}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}

import java.net.URI
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*

/**
 * Represents a page to be crawled.
 *
 * There should be exactly one [[Page]] actor per page. [[Page]] actors should be managed by a [[PageManager]] actor.
 *
 * This actor is stateful, sharded, gracefully passivated and persisted.
 *
 * Entity ID: URL of the page.
 */
object Page {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("Page")

  sealed trait Command
  //Persistence commands (these have to be part of the public protocol so that they work with ShardingEnvelopes):
  case class RecoveryResult(result: Option[PageEntity]) extends Command
  case object InsertSuccess extends Command
  case object UpdateSuccess extends Command
  //Domain commands:
  case class Discover(crawlDepth: Int) extends Command
  case object Success extends Command
  case object Redirect extends Command
  case object Error extends Command
  //Internal commands:
  private case object Passivate extends Command

  def apply(entityContext: EntityContext[Command], persistence: ActorRef[PagePersistence.Command]): Behavior[Command] = Behaviors.setup(context => {
    Behaviors.withStash(100)(buffer => {
      val config = context.system.settings.config
      val receiveTimeout = config.getDuration("abwcf.page.passivation-receive-timeout").toScala
      val hostQueueShardRegion = HostQueue.getShardRegion(context.system)

      new Page(hostQueueShardRegion, persistence, receiveTimeout, entityContext.shard, context, buffer).recovering(entityContext.entityId)
    })
  })

  def getShardRegion(system: ActorSystem[?], persistence: ActorRef[PagePersistence.Command]): ActorRef[ShardingEnvelope[Command]] = {
    ClusterSharding(system).init(Entity(TypeKey)(entityContext => Page(entityContext, persistence))) //TODO: Disable remember entities.
  }
}

private class Page private (hostQueueShardRegion: ActorRef[ShardingEnvelope[HostQueue.Command]],
                            persistence: ActorRef[PagePersistence.Command],
                            receiveTimeout: FiniteDuration,
                            shard: ActorRef[ClusterSharding.ShardCommand],
                            context: ActorContext[Page.Command],
                            buffer: StashBuffer[Page.Command]) {
  import Page.*

  /**
   * Adds the URL to a [[HostQueue]] so that it can be fetched.
   */
  private def addToHostQueue(page: PageEntity): Unit = {
    val host = URI(page.url).getHost
    hostQueueShardRegion ! ShardingEnvelope(host, HostQueue.Enqueue(page.url, page.crawlDepth))
  }
  
  private def recovering(url: String): Behavior[Command] = {
    persistence ! PagePersistence.Recover(url)
    
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
      persistence ! PagePersistence.Insert(page)
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
      case Discover(_) => Behaviors.same //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.
      
      case Success | Redirect | Error =>
        persistence ! PagePersistence.UpdateStatus(page.url, PageStatus.Processed)
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
