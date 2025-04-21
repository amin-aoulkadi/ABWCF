package abwcf.actors.persistence.host

import abwcf.actors.HostManager
import abwcf.actors.persistence.host.HostPersistence.Recover
import abwcf.data.HostInformation
import abwcf.persistence.HostRepository
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.util.{Failure, Success}

object HostReader {
  sealed trait Command
  private case class RecoverSuccess(result: Option[HostInformation], replyToSchemeAndAuthority: String) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(hostRepository: HostRepository): Behavior[Command | HostPersistence.ReadCommand] = Behaviors.setup(context => {
    val sharding = ClusterSharding(context.system)
    
    Behaviors.receiveMessage({
      case Recover(schemeAndAuthority) =>
        context.pipeToSelf(hostRepository.findBySchemeAndAuthority(schemeAndAuthority))({
          case Success(result) => RecoverSuccess(result, schemeAndAuthority)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case RecoverSuccess(result, replyToSchemeAndAuthority) =>
        val hostManager = sharding.entityRefFor(HostManager.TypeKey, replyToSchemeAndAuthority)
        hostManager ! HostManager.RecoveryResult(result)
        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Exception while inserting", throwable)
        Behaviors.same
    })
  })
}
