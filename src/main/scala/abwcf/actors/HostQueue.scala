package abwcf.actors

import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import org.apache.pekko.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}

import java.time.{Duration, Instant}
import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*

/**
 * Manages the crawl delay for a host.
 *
 * There should be exactly one [[HostQueue]] actor per crawled host.
 *
 * This actor is stateful, sharded, gracefully passivated and registered with the receptionist.
 *
 * Entity ID: Domain name or IP address of the host.
 *
 * This entity will not be remembered (even if remembering entities is enabled).
 */
object HostQueue { //TODO: HostQueues are not persisted so they reset after shard rebalancing.
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("HostQueue")
  val HQServiceKey: ServiceKey[Command] = ServiceKey("HostQueue")

  sealed trait Command
  case class Enqueue(url: String, crawlDepth: Int) extends Command
  case class GetHead(replyTo: ActorRef[Reply]) extends Command
  private case object Passivate extends Command

  sealed trait Reply
  case class Head(url: String, crawlDepth: Int) extends Reply
  case object Unavailable extends Reply

  def apply(shard: ActorRef[ClusterSharding.ShardCommand]): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val crawlDelay = config.getDuration("abwcf.host-queue.crawl-delay")
    val receiveTimeout = config.getDuration("abwcf.host-queue.passivation-receive-timeout").toScala

    new HostQueue(crawlDelay, receiveTimeout, shard, context).emptyQueue(Instant.MIN)
  })

  def getShardRegion(system: ActorSystem[?]): ActorRef[ShardingEnvelope[Command]] = {
    val settings = ClusterShardingSettings(system).withRememberEntities(false) //HostQueue state is not persisted, so it doesn't make sense to remember HostQueue entities.

    ClusterSharding(system).init(
      Entity(TypeKey)(entityContext => HostQueue(entityContext.shard))
        .withSettings(settings)
    )
  }
}

private class HostQueue private (crawlDelay: Duration,
                                 receiveTimeout: FiniteDuration,
                                 shard: ActorRef[ClusterSharding.ShardCommand],
                                 context: ActorContext[HostQueue.Command]) {
  import HostQueue.*

  private def queue(urls: Queue[(String, Int)], crawlDelayEnd: Instant): Behavior[Command] = {
    //Disable passivation and register with the receptionist:
    context.cancelReceiveTimeout() //Non-empty HostQueues should not be passivated.
    context.system.receptionist ! Receptionist.Register(HQServiceKey, context.self) //Allows the HostQueueRouter to route messages to this HostQueue.

    Behaviors.receiveMessage({
      case Enqueue(url, crawlDepth) => queue(urls.enqueue((url, crawlDepth)), crawlDelayEnd)

      case GetHead(replyTo) if Instant.now.isAfter(crawlDelayEnd) =>
        val (head, tail) = urls.dequeue
        replyTo ! Head(head._1, head._2)

        if (tail.isEmpty) {
          context.system.receptionist ! Receptionist.Deregister(HQServiceKey, context.self) //The HostQueueRouter should stop routing messages to this HostQueue.
          emptyQueue(Instant.now.plus(crawlDelay))
        } else {
          queue(tail, Instant.now.plus(crawlDelay))
        }

      case GetHead(replyTo) =>
        replyTo ! Unavailable
        Behaviors.same

      case Passivate =>
        context.log.info("Skipping unexpected message {}", Passivate)
        Behaviors.same
    })
  }

  private def emptyQueue(crawlDelayEnd: Instant): Behavior[Command] = {
    context.setReceiveTimeout(receiveTimeout, Passivate) //Enable passivation. Empty HostQueues can be passivated (but not immediately as there may still be messages in the mailbox).

    Behaviors.receiveMessage({
      case Enqueue(url, crawlDepth) => queue(Queue((url, crawlDepth)), crawlDelayEnd)

      case GetHead(replyTo) =>
        replyTo ! Unavailable
        Behaviors.same

      case Passivate =>
        shard ! ClusterSharding.Passivate(context.self)
        Behaviors.same
    })
  }
}
