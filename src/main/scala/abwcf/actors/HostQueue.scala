package abwcf.actors

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.pattern.StatusReply

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

  sealed trait Command
  case class Enqueue(url: String) extends Command
  case class GetUrl(replyTo: ActorRef[StatusReply[String]]) extends Command

  private case class State(
                            urls: Queue[String],
                            crawlDelayEnd: Instant
                          )

  def apply(): Behavior[Command] = statefulBehavior(State(Queue.empty, Instant.MIN)) //TODO: Notify the Balancer actors that there is a new HostQueue.

  private def statefulBehavior(state: State): Behavior[Command] = Behaviors.receiveMessage({
    case Enqueue(url) =>
      statefulBehavior(state.copy(urls = state.urls.enqueue(url)))

    case GetUrl(replyTo) =>
      if (state.urls.nonEmpty && Instant.now.isAfter(state.crawlDelayEnd)) {
        val (head, tail) = state.urls.dequeue
        replyTo ! StatusReply.Success(head)

        if (tail.isEmpty) {
          Behaviors.stopped //TODO: What about messages that are already in or in flight to the inbox of this actor?
        } else {
          statefulBehavior(State(tail, Instant.now.plusSeconds(1))) //TODO: Maybe wait until the page has been fetched?
        }
      } else {
        replyTo ! StatusReply.Error("No URL available.")
        Behaviors.same
      }
  })
}
