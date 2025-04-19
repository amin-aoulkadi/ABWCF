package abwcf.actors

import abwcf.actors.persistence.host.{HostPersistence, HostPersistenceManager}
import org.apache.pekko.actor.typed.{Behavior, SupervisorStrategy}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

/**
 * Gateway between [[HostManager]] actors and non-sharded actors.
 *
 * There should be one [[HostGateway]] actor per node.
 *
 * This actor is stateless.
 */
object HostGateway {
  sealed trait Command

  type CombinedCommand = Command | HostPersistence.Command

  def apply(): Behavior[CombinedCommand] = Behaviors.setup(context => {
    val hostShardRegion = HostManager.getShardRegion(context.system, context.self)
    
    val hostPersistenceManager = context.spawn(
      Behaviors.supervise(HostPersistenceManager(hostShardRegion))
        .onFailure(SupervisorStrategy.resume), //Restarting would be problematic because the HostPersistenceManager internally creates a SlickSession that has to be closed explicitly.
      "host-persistence-manager"
    )
    
    Behaviors.receiveMessage({
      case command: HostPersistence.Command =>
        hostPersistenceManager ! command
        Behaviors.same
    })
  })
}
