package abwcf.actors.persistence

import abwcf.PageEntity
import abwcf.actors.Page
import abwcf.persistence.PageRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

import scala.util.{Failure, Success}

object PageInserter {
  sealed trait Command
  case class Insert(page: PageEntity) extends Command
  private case class FutureSuccess(url: String) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command
  
  sealed trait Reply
  case object InsertSuccess extends Reply

  def apply(pageRepository: PageRepository, pageShardRegion: ActorRef[ShardingEnvelope[Page.Command]]): Behavior[Command] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case Insert(page) =>
        context.pipeToSelf(pageRepository.insert(page))({
          case Success(_) => FutureSuccess(page.url)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FutureSuccess(url) =>
        pageShardRegion ! ShardingEnvelope(url, Page.InsertSuccess)
        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Error while inserting", throwable)
        Behaviors.same
    })
  })
}
