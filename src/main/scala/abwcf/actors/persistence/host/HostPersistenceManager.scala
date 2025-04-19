package abwcf.actors.persistence.host

import abwcf.actors.HostManager
import abwcf.actors.persistence.host.HostPersistence.{Insert, Recover, Update}
import abwcf.persistence.SlickHostRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.slick.scaladsl.SlickSession

/**
 * Manages communication with the [[abwcf.data.HostInformation]] database.
 *
 * There should be one [[HostPersistenceManager]] actor per node.
 *
 * This actor uses a [[SlickSession]] internally, which must be closed explicitly to avoid leaking database resources.
 */
object HostPersistenceManager {
  def apply(hostShardRegion: ActorRef[ShardingEnvelope[HostManager.Command]]): Behavior[HostPersistence.Command] = Behaviors.setup(context => {
    val session = SlickSession.forConfig("postgres-slick") //TODO: Add to config.
    val materializer = Materializer(context)
    val hostRepository = new SlickHostRepository()(using session, materializer)

    val hostInserter = context.spawnAnonymous(HostInserter(hostRepository, hostShardRegion)) //TODO: Supervise.
    val hostReader = context.spawnAnonymous(HostReader(hostRepository, hostShardRegion))
    val hostUpdater = context.spawnAnonymous(HostUpdater(hostRepository, hostShardRegion))

    context.system.classicSystem.registerOnTermination(() => session.close()) //TODO: Manage the session elsewhere.

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
