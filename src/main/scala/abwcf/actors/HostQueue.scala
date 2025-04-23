package abwcf.actors

import abwcf.data.Page
import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey as ReceptionistServiceKey}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import org.apache.pekko.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}

import java.time.{Duration, Instant}
import scala.collection.mutable
import scala.compiletime.uninitialized
import scala.jdk.DurationConverters.*

/**
 * Manages the crawl delay for a host and sorts pages by crawl priority.
 *
 * There should be exactly one [[HostQueue]] actor per crawled host.
 *
 * This actor is stateful, sharded, gracefully passivated and registered with the receptionist.
 *
 * Entity ID: Scheme and authority components of a URL.
 *
 * This entity will not be remembered (even if remembering entities is enabled).
 */
object HostQueue {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("HostQueue")
  val ServiceKey: ReceptionistServiceKey[Command] = ReceptionistServiceKey("HostQueue")

  sealed trait Command
  case class Enqueue(page: Page) extends Command
  case class GetHead(replyTo: ActorRef[Reply]) extends Command
  private case object Passivate extends Command

  sealed trait Reply
  case class Head(page: Page) extends Reply
  case class Unavailable(until: Instant) extends Reply

  private type CombinedCommand = Command | HostManager.HostInfo

  def apply(entityContext: EntityContext[Command]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    new HostQueue(entityContext.entityId, entityContext.shard, context).requestHostInfo()
  }).narrow

  def initializeSharding(system: ActorSystem[?]): ActorRef[ShardingEnvelope[Command]] = {
    val settings = ClusterShardingSettings(system)
      .withRememberEntities(false) //HostQueue state is not persisted, so it doesn't make sense to remember HostQueue entities.
      .withNoPassivationStrategy() //Disable automatic passivation.

    ClusterSharding(system).init(
      Entity(TypeKey)(entityContext => HostQueue(entityContext))
        .withSettings(settings)
    )
  }
}

private class HostQueue private (schemeAndAuthority: String,
                                 shard: ActorRef[ClusterSharding.ShardCommand],
                                 context: ActorContext[HostQueue.CombinedCommand]) {
  import HostQueue.*

  private val sharding = ClusterSharding(context.system)
  private val config = context.system.settings.config
  private val receiveTimeout = config.getDuration("abwcf.actors.host-queue.passivation-receive-timeout").toScala

  private val queue = mutable.PriorityQueue.empty[Page](using Ordering.by(_.crawlPriority)) //Mutable state!
  private var crawlDelay: Duration = uninitialized //Mutable state!
  private var crawlDelayEnd = Instant.MIN //Mutable state!

  private def requestHostInfo(): Behavior[CombinedCommand] = {
    //Request information from the HostManager:
    val hostManager = sharding.entityRefFor(HostManager.TypeKey, schemeAndAuthority)
    hostManager ! HostManager.GetHostInfo(context.self) //TODO: Better use ask so that there is a timeout and retry.

    Behaviors.receiveMessage({
      case HostManager.HostInfo(hostInfo) => //TODO: Handle HostInformation expiry.
        crawlDelay = Duration.ofMillis(hostInfo.robotRules.getCrawlDelay)

        if (queue.isEmpty) {
          emptyQueue()
        } else {
          nonEmptyQueue()
        }

      case Enqueue(page) =>
        queue.enqueue(page)
        Behaviors.same

      case GetHead(replyTo) =>
        replyTo ! Unavailable(Instant.MAX)
        Behaviors.same

      case Passivate =>
        context.log.warn("Skipping unexpected message {}", Passivate)
        Behaviors.same
    })
  }

  private def nonEmptyQueue(): Behavior[CombinedCommand] = {
    //Disable passivation and register with the receptionist:
    context.cancelReceiveTimeout() //Non-empty HostQueues should not be passivated.
    context.system.receptionist ! Receptionist.Register(ServiceKey, context.self) //Allows the HostQueueRouter to route messages to this HostQueue.

    Behaviors.receiveMessage({
      case Enqueue(page) =>
        queue.enqueue(page)
        Behaviors.same

      case GetHead(replyTo) if Instant.now.isAfter(crawlDelayEnd) =>
        val head = queue.dequeue
        replyTo ! Head(head)
        crawlDelayEnd = Instant.now.plus(crawlDelay)

        if (queue.isEmpty) {
          context.system.receptionist ! Receptionist.Deregister(ServiceKey, context.self) //The HostQueueRouter should stop routing messages to this HostQueue.
          emptyQueue()
        } else {
          Behaviors.same
        }

      case GetHead(replyTo) =>
        replyTo ! Unavailable(crawlDelayEnd)
        Behaviors.same

      case other =>
        context.log.warn("Skipping unexpected message {}", other)
        Behaviors.same
    })
  }

  private def emptyQueue(): Behavior[CombinedCommand] = {
    context.setReceiveTimeout(receiveTimeout, Passivate) //Enable passivation. Empty HostQueues can be passivated (but not immediately as there may still be messages in the mailbox).

    Behaviors.receiveMessage({
      case Enqueue(page) =>
        queue.enqueue(page)
        nonEmptyQueue()

      case GetHead(replyTo) =>
        replyTo ! Unavailable(Instant.MAX)
        Behaviors.same

      case Passivate =>
        shard ! ClusterSharding.Passivate(context.self)
        Behaviors.same

      case other =>
        context.log.warn("Skipping unexpected message {}", other)
        Behaviors.same
    })
  }
}
