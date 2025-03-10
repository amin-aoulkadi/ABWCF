package abwcf.actors

import abwcf.actors.Page.Status.{Discovered, Undefined}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}

import java.net.URI

/**
 * Represents a page to be crawled.
 *
 * There should be exactly one [[Page]] actor per page.
 *
 * This actor is stateful and sharded.
 *
 * Entity ID: URL of the page.
 */
object Page {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("Page")

  enum Status:
    case Undefined, Discovered
  
  sealed trait Command
  case object Discover extends Command
  
  def apply(url: String): Behavior[Command] = Behaviors.setup(context => {
    val hostQueueShardRegion = ClusterSharding(context.system).init(Entity(HostQueue.TypeKey)(_ => HostQueue()))
    new Page(url, hostQueueShardRegion).page(Undefined)
  })
}

private class Page private (url: String,
                    hostQueueShardRegion: ActorRef[ShardingEnvelope[HostQueue.Command]]) {
  import Page.*
  
  private def page(status: Status): Behavior[Command] = Behaviors.receiveMessage({
    case Discover if status == Undefined =>
      //Add the page to a HostQueue so that it can be fetched:
      val host = URI(url).getHost
      hostQueueShardRegion ! ShardingEnvelope(host, HostQueue.Enqueue(url))
      page(Discovered)

    case Discover => //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.
      Behaviors.same
  })
}
