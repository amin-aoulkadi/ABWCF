package abwcf.actors.persistence.page

import abwcf.actors.PageManager
import abwcf.actors.persistence.page.PagePersistence.Insert
import abwcf.data.Page
import abwcf.persistence.PageRepository
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.collection.mutable
import scala.jdk.DurationConverters.*
import scala.util.{Failure, Success}

object PageInserter {
  sealed trait Command
  private case object StartInserting extends Command
  private case object FutureSuccess extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(pageRepository: PageRepository): Behavior[Command | PagePersistence.InsertCommand] = Behaviors.setup(context => {
    Behaviors.withTimers(timers => {
      new PageInserter(pageRepository, context, timers).prepareNextBatch()
    })
  })
}

private class PageInserter private (pageRepository: PageRepository,
                                    context: ActorContext[PageInserter.Command | PagePersistence.InsertCommand],
                                    timers: TimerScheduler[PageInserter.Command | PagePersistence.InsertCommand]) {
  import PageInserter.*

  private val sharding = ClusterSharding(context.system)
  private val config = context.system.settings.config
  private val maxBatchSize = config.getInt("abwcf.persistence.page.insert.max-batch-size")
  private val maxBatchDelay = config.getDuration("abwcf.persistence.page.insert.max-batch-delay").toScala

  private var currentBatch = mutable.ListBuffer.empty[Page] //Mutable state!
  private var pendingPages = mutable.ListBuffer.empty[Page] //Mutable state!

  private def prepareNextBatch(): Behavior[Command | PagePersistence.InsertCommand] = {
    val (head, tail) = pendingPages.splitAt(maxBatchSize)
    currentBatch = head
    pendingPages = tail

    if (currentBatch.size < maxBatchSize) {
      if (currentBatch.nonEmpty) {
        timers.startSingleTimer(StartInserting, maxBatchDelay) //Ensure that the current batch is inserted after maxBatchDelay.
      }

      collecting()
    } else { //The current batch is full.
      inserting()
    }
  }

  private def collecting(): Behavior[Command | PagePersistence.InsertCommand] = Behaviors.receiveMessage({
    case Insert(page) if currentBatch.isEmpty =>
      currentBatch.append(page)
      timers.startSingleTimer(StartInserting, maxBatchDelay) //Ensure that the current batch is inserted after maxBatchDelay.
      Behaviors.same

    case Insert(page) =>
      currentBatch.append(page)

      if (currentBatch.size < maxBatchSize) {
        Behaviors.same
      } else { //The current batch is full.
        inserting()
      }

    case StartInserting => inserting()

    case other =>
      context.log.warn("Skipping unexpected message {}", other)
      Behaviors.same
  })

  private def inserting(): Behavior[Command | PagePersistence.InsertCommand] = {
    context.pipeToSelf(pageRepository.insert(currentBatch))({
      case Success(_) => FutureSuccess
      case Failure(throwable) => FutureFailure(throwable)
    })

    Behaviors.receiveMessage({
      case FutureSuccess =>
        //Notify the PageManagers:
        currentBatch.map(_.url)
          .foreach(url => {
            val pageManager = sharding.entityRefFor(PageManager.TypeKey, url)
            pageManager ! PageManager.InsertSuccess
          })

        prepareNextBatch()

      case FutureFailure(throwable) => //TODO: Handle RejectedExecutionException?
        context.log.error("Exception while inserting", throwable)
        prepareNextBatch()

      case Insert(page) =>
        pendingPages.append(page)
        Behaviors.same

      case StartInserting => Behaviors.same
    })
  }
}
