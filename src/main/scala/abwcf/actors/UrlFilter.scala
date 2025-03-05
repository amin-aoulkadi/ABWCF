package abwcf.actors

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

/**
 * Applies black- and whitelist filtering to discard URLs that should not be crawled.
 * 
 * This actor is stateless.
 */
object UrlFilter {
  sealed trait Command
  case class Filter(url: String) extends Command

  def apply(pageManager: ActorRef[PageManager.Command]): Behavior[Command] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case Filter(url) =>
        //TODO: Implement URL filtering.
        pageManager ! PageManager.Spawn(url)
        Behaviors.same
    })
  })
}
