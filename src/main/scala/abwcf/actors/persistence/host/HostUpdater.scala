package abwcf.actors.persistence.host

import abwcf.actors.HostManager
import abwcf.actors.persistence.host.HostPersistence.Update
import abwcf.persistence.HostRepository
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.util.{Failure, Success}

object HostUpdater {
  sealed trait Command
  private case class FutureSuccess(schemeAndAuthority: String) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(hostRepository: HostRepository): Behavior[Command | HostPersistence.UpdateCommand] = Behaviors.setup(context => {
    val sharding = ClusterSharding(context.system)
    
    Behaviors.receiveMessage({
      case Update(hostInfo) =>
        context.pipeToSelf(hostRepository.update(hostInfo))({
          case Success(_) => FutureSuccess(hostInfo.schemeAndAuthority)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FutureSuccess(schemeAndAuthority) =>
        val hostManager = sharding.entityRefFor(HostManager.TypeKey, schemeAndAuthority)
        hostManager ! HostManager.UpdateSuccess
        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Exception while updating", throwable)
        Behaviors.same
    })
  })
}
