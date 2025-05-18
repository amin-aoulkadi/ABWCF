package abwcf.actors.persistence.host

import abwcf.actors.HostManager
import abwcf.actors.persistence.host.HostPersistence.Recover
import abwcf.data.HostInformation
import abwcf.persistence.HostRepository
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
 * Handles [[HostInformation]]-related `SELECT` queries.
 *
 * There should be one [[HostReader]] actor per node.
 *
 * This actor is stateful.
 */
object HostReader {
  sealed trait Command
  private case class RecoverSuccess(result: Option[HostInformation], replyToSchemeAndAuthority: String) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(hostRepository: HostRepository): Behavior[Command | HostPersistence.ReadCommand] = Behaviors.setup(context => {
    new HostReader(hostRepository, context).hostReader()
  })
}

private class HostReader private (hostRepository: HostRepository, context: ActorContext[HostReader.Command | HostPersistence.ReadCommand]) {
  import HostReader.*

  private val sharding = ClusterSharding(context.system)
  private val config = context.system.settings.config
  private val maxActiveQueries = config.getInt("abwcf.persistence.host.select.max-active-recovery-queries")

  private val pendingRecoveries = mutable.Queue.empty[String] //Mutable state!
  private var activeQueries = 0 //Mutable state!

  private def hostReader(): Behavior[Command | HostPersistence.ReadCommand] = Behaviors.receiveMessage({
    case Recover(schemeAndAuthority) =>
      if (activeQueries < maxActiveQueries) {
        //Start the query:
        context.pipeToSelf(hostRepository.findBySchemeAndAuthority(schemeAndAuthority))({
          case Success(result) => RecoverSuccess(result, schemeAndAuthority)
          case Failure(throwable) => FutureFailure(throwable)
        })

        activeQueries += 1
      } else {
        pendingRecoveries.enqueue(schemeAndAuthority)
      }

      Behaviors.same

    case RecoverSuccess(result, replyToSchemeAndAuthority) =>
      val hostManager = sharding.entityRefFor(HostManager.TypeKey, replyToSchemeAndAuthority)
      hostManager ! HostManager.RecoveryResult(result)
      startNextQuery()
      Behaviors.same

    case FutureFailure(throwable) =>
      context.log.error("Exception while reading", throwable)
      startNextQuery()
      Behaviors.same
  })

  private def startNextQuery(): Unit = {
    if (pendingRecoveries.nonEmpty) {
      //Start the next query:
      val schemeAndAuthority = pendingRecoveries.dequeue()

      context.pipeToSelf(hostRepository.findBySchemeAndAuthority(schemeAndAuthority))({
        case Success(result) => RecoverSuccess(result, schemeAndAuthority)
        case Failure(throwable) => FutureFailure(throwable)
      })
    } else {
      activeQueries -= 1
    }
  }
}
