package abwcf.actors.persistence.host

import abwcf.actors.HostManager
import abwcf.actors.persistence.host.HostPersistence.Insert
import abwcf.persistence.HostRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

import scala.util.{Failure, Success}

object HostInserter { //TODO: Batch inserts.
  sealed trait Command
  private case class FutureSuccess(schemeAndAuthority: String) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(hostRepository: HostRepository, hostShardRegion: ActorRef[ShardingEnvelope[HostManager.Command]]): Behavior[Command | HostPersistence.InsertCommand] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case Insert(hostInfo) =>
        context.pipeToSelf(hostRepository.insert(hostInfo))({
          case Success(_) => FutureSuccess(hostInfo.schemeAndAuthority)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FutureSuccess(schemeAndAuthority) =>
        hostShardRegion ! ShardingEnvelope(schemeAndAuthority, HostManager.InsertSuccess)
        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Exception while inserting", throwable)
        Behaviors.same
    })
  })
}
