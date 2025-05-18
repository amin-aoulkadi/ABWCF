package abwcf.actors.persistence.page

import abwcf.actors.PageManager
import abwcf.actors.persistence.page.PagePersistence.{FindByStatus, Recover, ResultSeq}
import abwcf.data.Page
import abwcf.persistence.PageRepository
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
 * Handles [[Page]]-related `SELECT` queries.
 *
 * There should be one [[PageReader]] actor per node.
 *
 * This actor is stateful.
 *
 * @note Under the hood, database communication is handled by a [[java.util.concurrent.Executor]] with a size-limited task queue. The following problem was discovered during testing:<br>
 *       If every recovery query was started immediately upon arrival of the [[Recover]] message, the executor's task queue would fill up. Once it was full, all excess queries (not just recovery queries, but any type of database query) immediately failed with a [[java.util.concurrent.RejectedExecutionException]].<br>
 *       To avoid completely filling the executor's task queue, the [[PageReader]] limits the number of active recovery queries and buffers excess recovery queries in its own unbounded queue.
 */
object PageReader {
  sealed trait Command
  private case class RecoverSuccess(result: Option[Page], replyToUrl: String) extends Command
  private case class FindByStatusSuccess(result: Seq[Page], replyTo: ActorRef[ResultSeq]) extends Command
  private case class RecoverFailure(throwable: Throwable) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(pageRepository: PageRepository): Behavior[Command | PagePersistence.ReadCommand] = Behaviors.setup(context => {
    new PageReader(pageRepository, context).pageReader()
  })
}

private class PageReader private (pageRepository: PageRepository, context: ActorContext[PageReader.Command | PagePersistence.ReadCommand]) {
  import PageReader.*

  private val sharding = ClusterSharding(context.system)
  private val config = context.system.settings.config
  private val maxActiveQueries = config.getInt("abwcf.persistence.page.select.max-active-recovery-queries")

  private val pendingRecoveries = mutable.Queue.empty[String] //Mutable state!
  private var activeQueries = 0 //Mutable state!

  private def pageReader(): Behavior[Command | PagePersistence.ReadCommand] = Behaviors.receiveMessage({
    case Recover(url) =>
      if (activeQueries < maxActiveQueries) {
        //Start the query:
        context.pipeToSelf(pageRepository.findByUrl(url))({
          case Success(result) => RecoverSuccess(result, url)
          case Failure(throwable) => RecoverFailure(throwable)
        })

        activeQueries += 1
      } else {
        pendingRecoveries.enqueue(url)
      }

      Behaviors.same

    case FindByStatus(status, limit, replyTo) => //Not subject to maxActiveQueries because this query is executed infrequently.
      context.pipeToSelf(pageRepository.findByStatusOrderByCrawlPriorityDesc(status, limit))({
        case Success(result) => FindByStatusSuccess(result, replyTo)
        case Failure(throwable) => FutureFailure(throwable)
      })
      Behaviors.same

    case RecoverSuccess(result, replyToUrl) =>
      val pageManager = sharding.entityRefFor(PageManager.TypeKey, replyToUrl)
      pageManager ! PageManager.RecoveryResult(result)
      startNextQuery()
      Behaviors.same

    case FindByStatusSuccess(result, replyTo) =>
      replyTo ! ResultSeq(result)
      Behaviors.same

    case RecoverFailure(throwable) =>
      context.log.error("Exception while reading", throwable)
      startNextQuery()
      Behaviors.same

    case FutureFailure(throwable) =>
      context.log.error("Exception while reading", throwable)
      Behaviors.same
  })

  private def startNextQuery(): Unit = {
    if (pendingRecoveries.nonEmpty) {
      //Start the next query:
      val url = pendingRecoveries.dequeue()

      context.pipeToSelf(pageRepository.findByUrl(url))({
        case Success(result) => RecoverSuccess(result, url)
        case Failure(throwable) => RecoverFailure(throwable)
      })
    } else {
      activeQueries -= 1
    }
  }
}
