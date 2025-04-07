package abwcf.actors

import org.apache.pekko.actor.typed.scaladsl.{GroupRouter, Routers}

/**
 * A group router that routes messages to [[HostQueue]] actors. Uses the receptionist to discover [[HostQueue]] actors.
 *
 * There should be one [[HostQueueRouter]] actor per node.
 *
 * This actor is stateless.
 */
object HostQueueRouter {
  def apply(): GroupRouter[HostQueue.Command] = Routers.group(HostQueue.ServiceKey).withRandomRouting()
}
