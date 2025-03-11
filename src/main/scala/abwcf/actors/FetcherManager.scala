package abwcf.actors

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

/**
 * Manages [[Fetcher]] actors.
 *
 * There should be one [[FetcherManager]] actor per node.
 *
 * This actor is stateless.
 */
object FetcherManager {
  def apply(hostQueueRouter: ActorRef[HostQueue.Command]): Behavior[Nothing] = Behaviors.setup(context => {
    //TODO: Implement logic.
    context.spawnAnonymous(Fetcher(hostQueueRouter))
    context.spawnAnonymous(Fetcher(hostQueueRouter))
    Behaviors.ignore
  })
}
