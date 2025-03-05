package abwcf.actors

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey

/**
 * Represents a page to be crawled.
 * 
 * This actor is sharded.
 */
object Page {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("Page")
  
  sealed trait Command
  case object Start extends Command
  
  def apply(url: String): Behavior[Command] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case Start =>
        context.log.info(url)
        Behaviors.same
    })
  })
}
