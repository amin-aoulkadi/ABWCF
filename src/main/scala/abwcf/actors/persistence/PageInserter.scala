package abwcf.actors.persistence

import abwcf.actors.PageManager
import abwcf.actors.persistence.PagePersistence.Insert
import abwcf.persistence.PageRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

import scala.util.{Failure, Success}

object PageInserter { //TODO: Batch inserts.
  sealed trait Command
  private case class FutureSuccess(url: String) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(pageRepository: PageRepository, pageShardRegion: ActorRef[ShardingEnvelope[PageManager.Command]]): Behavior[Command | PagePersistence.InsertCommand] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case Insert(page) =>
        context.pipeToSelf(pageRepository.insert(page))({
          case Success(_) => FutureSuccess(page.url)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FutureSuccess(url) =>
        pageShardRegion ! ShardingEnvelope(url, PageManager.InsertSuccess)
        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Exception while inserting", throwable)
        Behaviors.same
    })
  })
}
