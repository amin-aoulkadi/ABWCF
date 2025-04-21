package abwcf.actors.persistence.page

import abwcf.actors.PageManager
import abwcf.actors.persistence.page.PagePersistence.Insert
import abwcf.persistence.PageRepository
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.util.{Failure, Success}

object PageInserter { //TODO: Batch inserts.
  sealed trait Command
  private case class FutureSuccess(url: String) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(pageRepository: PageRepository): Behavior[Command | PagePersistence.InsertCommand] = Behaviors.setup(context => {
    val sharding = ClusterSharding(context.system)

    Behaviors.receiveMessage({
      case Insert(page) =>
        context.pipeToSelf(pageRepository.insert(page))({
          case Success(_) => FutureSuccess(page.url)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FutureSuccess(url) =>
        val pageManager = sharding.entityRefFor(PageManager.TypeKey, url)
        pageManager ! PageManager.InsertSuccess
        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Exception while inserting", throwable)
        Behaviors.same
    })
  })
}
