package abwcf.actors

import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey

import java.time.Instant
import scala.collection.immutable.Queue

/**
 * Manages the crawl delay for a host.
 *
 * There should be exactly one [[HostQueue]] actor per crawled host.
 *
 * This actor is stateful and sharded.
 *
 * Entity ID: Domain name or IP address of the host.
 */
object HostQueue { //TODO: HostQueues must be exempt from (automatic) passivation!
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("HostQueue")
  val HostQueueServiceKey: ServiceKey[Command] = ServiceKey("HostQueue")

  sealed trait Command
  case class Enqueue(url: String) extends Command
  case class GetHead(replyTo: ActorRef[Reply]) extends Command

  sealed trait Reply
  case class Head(url: String) extends Reply
  case object Unavailable extends Reply

  def apply(): Behavior[Command] = Behaviors.setup(context => {
    //TODO: Read crawl delay from config.
    context.system.receptionist ! Receptionist.Register(HostQueueServiceKey, context.self) //Caution: According to the Pekko documentation, the scalability of the receptionist is limited. This could be a problem for crawls with a large number of hosts.
    new HostQueue(context).hostQueue(Queue.empty, Instant.MIN)
  })
}

private class HostQueue private (context: ActorContext[HostQueue.Command]) {
  import HostQueue.*

  private def hostQueue(urls: Queue[String], crawlDelayEnd: Instant): Behavior[Command] = Behaviors.receiveMessage({
    case Enqueue(url) =>
      hostQueue(urls.enqueue(url), crawlDelayEnd)

    case GetHead(replyTo) if urls.nonEmpty && Instant.now.isAfter(crawlDelayEnd) =>
      val (head, tail) = urls.dequeue
      replyTo ! Head(head)
  
      if (tail.isEmpty) {
        //context.system.receptionist ! Receptionist.Deregister(HostQueueServiceKey, context.self)
        Behaviors.stopped //TODO: What about messages that are already in or in flight to the inbox of this actor?
      } else {
        hostQueue(tail, Instant.now.plusSeconds(1)) //TODO: Maybe wait until the page has been fetched?
      }
        
    case GetHead(replyTo) =>
      replyTo ! Unavailable
      Behaviors.same
  })
}
