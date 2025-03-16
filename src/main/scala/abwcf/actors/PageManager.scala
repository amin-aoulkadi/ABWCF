package abwcf.actors

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

/**
 * Creates [[Page]] actors.
 *
 * There should be one [[PageManager]] actor per node.
 *
 * This actor is stateless.
 */
object PageManager {
  sealed trait Command
  case class Spawn(url: String) extends Command

  def apply(): Behavior[Command] = Behaviors.setup(context => {
    val pageShardRegion = Page.getShardRegion(context.system)

    Behaviors.receiveMessage({
      case Spawn(url) => //TODO: Add database lookup (with a small cache).
        pageShardRegion ! ShardingEnvelope(url, Page.Discover)
        Behaviors.same
    })
  })
}
