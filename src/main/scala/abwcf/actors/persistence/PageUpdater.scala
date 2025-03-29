package abwcf.actors.persistence

import abwcf.actors.Page
import abwcf.persistence.PageRepository
import abwcf.{PageEntity, PageStatus}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

import scala.util.{Failure, Success}

object PageUpdater {
  sealed trait Command
  case class Update(page: PageEntity) extends Command
  case class UpdateStatus(url: String, status: PageStatus) extends Command
  private case class FutureSuccess(url: String) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(pageRepository: PageRepository, pageShardRegion: ActorRef[ShardingEnvelope[Page.Command]]): Behavior[Command] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case Update(page) =>
        context.pipeToSelf(pageRepository.update(page))({
          case Success(_) => FutureSuccess(page.url)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case UpdateStatus(url, status) =>
        context.pipeToSelf(pageRepository.updateStatus(url, status))({
          case Success(_) => FutureSuccess(url)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FutureSuccess(url) =>
        pageShardRegion ! ShardingEnvelope(url, Page.UpdateSuccess)
        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Error while updating", throwable)
        Behaviors.same
    })
  })
}
