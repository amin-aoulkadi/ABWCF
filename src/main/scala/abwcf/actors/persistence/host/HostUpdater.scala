package abwcf.actors.persistence.host

import abwcf.actors.HostManager
import abwcf.actors.persistence.Batcher
import abwcf.actors.persistence.host.HostPersistence.Update
import abwcf.data.HostInformation
import abwcf.persistence.HostRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{Behavior, SupervisorStrategy}
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.jdk.DurationConverters.*
import scala.util.{Failure, Success}

object HostUpdater {
  sealed trait Command
  private case class FutureSuccess(batch: Seq[HostInformation]) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  private type CombinedCommand = Command | HostPersistence.UpdateCommand | Batcher.Batch[HostInformation]

  def apply(hostRepository: HostRepository): Behavior[Command | HostPersistence.UpdateCommand] = Behaviors.setup[CombinedCommand](context => {
    val sharding = ClusterSharding(context.system)
    val config = context.system.settings.config
    val maxBatchSize = config.getInt("abwcf.persistence.slick.host.update.max-batch-size")
    val maxBatchDelay = config.getDuration("abwcf.persistence.slick.host.update.max-batch-delay").toScala

    val batcher = context.spawnAnonymous(
      Behaviors.supervise(Batcher(maxBatchSize, maxBatchDelay, context.self))
        .onFailure(SupervisorStrategy.resume) //Restarting would mean losing the current batch.
    )

    Behaviors.receiveMessage({
      case Update(hostInfo) =>
        batcher ! Batcher.Add(hostInfo)
        Behaviors.same

      case Batcher.Batch(batch: Seq[HostInformation] @unchecked) =>
        context.pipeToSelf(hostRepository.update(batch))({
          case Success(_) => FutureSuccess(batch)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FutureSuccess(batch) =>
        //Notify the HostManagers:
        batch.map(_.schemeAndAuthority)
          .foreach(schemeAndAuthority => {
            val hostManager = sharding.entityRefFor(HostManager.TypeKey, schemeAndAuthority)
            hostManager ! HostManager.UpdateSuccess
          })

        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Exception while updating", throwable)
        Behaviors.same
    })
  }).narrow
}
