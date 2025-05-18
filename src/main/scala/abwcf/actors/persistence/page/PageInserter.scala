package abwcf.actors.persistence.page

import abwcf.actors.PageManager
import abwcf.actors.persistence.Batcher
import abwcf.actors.persistence.page.PagePersistence.Insert
import abwcf.data.Page
import abwcf.persistence.PageRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.jdk.DurationConverters.*
import scala.util.{Failure, Success}

object PageInserter {
  sealed trait Command
  private case class FutureSuccess(batch: Seq[Page]) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  private type CombinedCommand = Command | PagePersistence.InsertCommand | Batcher.Batch[Page]

  def apply(pageRepository: PageRepository): Behavior[Command | PagePersistence.InsertCommand] = Behaviors.setup[CombinedCommand](context => {
    val sharding = ClusterSharding(context.system)
    val config = context.system.settings.config
    val maxBatchSize = config.getInt("abwcf.persistence.slick.page.insert.max-batch-size")
    val maxBatchDelay = config.getDuration("abwcf.persistence.slick.page.insert.max-batch-delay").toScala

    val batcher = context.spawnAnonymous(
      Behaviors.supervise(Batcher(maxBatchSize, maxBatchDelay, context.self))
        .onFailure(SupervisorStrategy.resume) //Restarting would mean losing the current batch.
    )

    Behaviors.receiveMessage({
      case Insert(page) =>
        batcher ! Batcher.Add(page)
        Behaviors.same

      case Batcher.Batch(batch: Seq[Page] @unchecked) =>
        context.pipeToSelf(pageRepository.insert(batch))({
          case Success(_) => FutureSuccess(batch)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FutureSuccess(batch) =>
        //Notify the PageManagers:
        batch.map(_.url)
          .foreach(url => {
            val pageManager = sharding.entityRefFor(PageManager.TypeKey, url)
            pageManager ! PageManager.InsertSuccess
          })

        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Exception while inserting", throwable)
        Behaviors.same
    })
  }).narrow
}
