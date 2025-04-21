package abwcf.actors.persistence.host

import abwcf.actors.persistence.host.HostPersistence.{Insert, Recover, Update}
import abwcf.persistence.SlickHostRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.stream.Materializer

/**
 * Manages communication with the [[abwcf.data.HostInformation]] database.
 *
 * There should be one [[HostPersistenceManager]] actor per node.
 *
 * This actor is stateless.
 */
object HostPersistenceManager {
  def apply(): Behavior[HostPersistence.Command] = Behaviors.setup(context => {
    val materializer = Materializer(context)
    val hostRepository = new SlickHostRepository()(using materializer)

    val hostInserter = context.spawnAnonymous(HostInserter(hostRepository)) //TODO: Supervise.
    val hostReader = context.spawnAnonymous(HostReader(hostRepository))
    val hostUpdater = context.spawnAnonymous(HostUpdater(hostRepository))

    Behaviors.receiveMessage({
      case Insert(hostInfo) =>
        hostInserter ! Insert(hostInfo)
        Behaviors.same

      case Update(hostInfo) =>
        hostUpdater ! Update(hostInfo)
        Behaviors.same

      case Recover(schemeAndAuthority) =>
        hostReader ! Recover(schemeAndAuthority)
        Behaviors.same
    })
  })
}
