package abwcf.actors.persistence.page

import abwcf.actors.PageManager
import abwcf.actors.persistence.Batcher
import abwcf.actors.persistence.page.PagePersistence.UpdateStatus
import abwcf.data.PageStatus
import abwcf.persistence.PageRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{Behavior, SupervisorStrategy}
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.jdk.DurationConverters.*
import scala.util.{Failure, Success}

object PageUpdater {
  sealed trait Command
  private case class FutureSuccess(batch: Seq[(String, PageStatus)]) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  private type CombinedCommand = Command | PagePersistence.UpdateCommand | Batcher.Batch[(String, PageStatus)]

  def apply(pageRepository: PageRepository): Behavior[Command | PagePersistence.UpdateCommand] = Behaviors.setup[CombinedCommand](context => {
    val sharding = ClusterSharding(context.system)
    val config = context.system.settings.config
    val maxBatchSize = config.getInt("abwcf.persistence.page.update.max-batch-size")
    val maxBatchDelay = config.getDuration("abwcf.persistence.page.update.max-batch-delay").toScala

    val batcher = context.spawnAnonymous(
      Behaviors.supervise(Batcher(maxBatchSize, maxBatchDelay, context.self))
        .onFailure(SupervisorStrategy.resume) //Restarting would mean losing the current batch.
    )

    Behaviors.receiveMessage({
      case UpdateStatus(url, status) =>
        batcher ! Batcher.Add((url, status))
        Behaviors.same

      case Batcher.Batch(batch: Seq[(String, PageStatus)] @unchecked) =>
        context.pipeToSelf(pageRepository.updateStatus(batch))({
          case Success(_) => FutureSuccess(batch)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FutureSuccess(batch) =>
        //Notify the PageManagers:
        batch.map(_._1)
          .foreach(url => {
            val pageManager = sharding.entityRefFor(PageManager.TypeKey, url)
            pageManager ! PageManager.UpdateSuccess
          })

        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Exception while updating", throwable)
        Behaviors.same
    })
  }).narrow
}
