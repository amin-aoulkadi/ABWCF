package abwcf.actors

import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}

import java.time.{Duration, Instant}
import scala.collection.immutable.Queue

/**
 * Manages the crawl delay for a host.
 *
 * There should be exactly one [[HostQueue]] actor per crawled host.
 *
 * This actor is stateful, sharded and registered with the receptionist.
 *
 * Entity ID: Domain name or IP address of the host.
 */
object HostQueue { //TODO: Handle (automatic) passivation and handle HostQueues being moved to a different node!
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("HostQueue")
  val HQServiceKey: ServiceKey[Command] = ServiceKey("HostQueue")

  sealed trait Command
  case class Enqueue(url: String) extends Command
  case class GetHead(replyTo: ActorRef[Reply]) extends Command

  sealed trait Reply
  case class Head(url: String) extends Reply
  case object Unavailable extends Reply

  def apply(): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val crawlDelay = config.getDuration("abwcf.host-queue.crawl-delay")

    context.system.receptionist ! Receptionist.Register(HQServiceKey, context.self)

    new HostQueue(crawlDelay).hostQueue(Queue.empty, Instant.MIN)
  })

  def getShardRegion(system: ActorSystem[?]): ActorRef[ShardingEnvelope[Command]] = {
    ClusterSharding(system).init(Entity(TypeKey)(_ => HostQueue()))
  }
}

private class HostQueue private (crawlDelay: Duration) {
  import HostQueue.*

  private def hostQueue(urls: Queue[String], crawlDelayEnd: Instant): Behavior[Command] = Behaviors.receiveMessage({
    case Enqueue(url) =>
      hostQueue(urls.enqueue(url), crawlDelayEnd)

    case GetHead(replyTo) if urls.nonEmpty && Instant.now.isAfter(crawlDelayEnd) =>
      val (head, tail) = urls.dequeue
      replyTo ! Head(head)
  
      if (tail.isEmpty) {
        Behaviors.stopped //TODO: What about messages that are already in or in flight to the inbox of this actor?
      } else {
        hostQueue(tail, Instant.now.plus(crawlDelay))
      }
        
    case GetHead(replyTo) =>
      replyTo ! Unavailable
      Behaviors.same
  })
}
