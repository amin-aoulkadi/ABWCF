package abwcf.actors

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}

/**
 * Creates [[Page]] actors.
 *
 * This actor is stateless.
 */
object PageManager {
  sealed trait Command
  case class Spawn(url: String) extends Command

  def apply(): Behavior[Command] = Behaviors.setup(context => {
    val pageShardRegion = ClusterSharding(context.system).init(Entity(Page.TypeKey)(entityContext => Page(entityContext.entityId)))

    Behaviors.receiveMessage({
      case Spawn(url) =>
        pageShardRegion ! ShardingEnvelope(url, Page.Start)
        Behaviors.same
    })
  })
}
