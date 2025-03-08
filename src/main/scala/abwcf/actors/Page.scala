package abwcf.actors

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}

import java.net.URI

/**
 * Represents a page to be crawled.
 *
 * There should be exactly one [[Page]] actor per page.
 * 
 * This actor is sharded.
 *
 * Entity ID: URL of the page.
 */
object Page {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("Page")
  
  sealed trait Command
  case class Discover(url: String) extends Command

  private case class State(
                            hostQueueShardRegion: ActorRef[ShardingEnvelope[HostQueue.Command]],
                            url: String
                          )
  
  def apply(): Behavior[Command] = Behaviors.setup(context => {
    val hostQueueShardRegion = ClusterSharding(context.system).init(Entity(HostQueue.TypeKey)(_ => HostQueue()))
    statefulBehavior(State(hostQueueShardRegion, null))
  })

  private def statefulBehavior(state: State): Behavior[Command] = Behaviors.receiveMessage({
    case Discover(url) =>
      if (state.url == null) { //The page has not been discovered yet.
        val host = URI(url).getHost
        state.hostQueueShardRegion ! ShardingEnvelope(host, HostQueue.Enqueue(url))
        statefulBehavior(state.copy(url = url))
      } else { //The page has already been discovered.
        Behaviors.same
      }
  })
}
