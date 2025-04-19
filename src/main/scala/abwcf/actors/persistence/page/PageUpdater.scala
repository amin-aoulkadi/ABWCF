package abwcf.actors.persistence.page

import abwcf.actors.PageManager
import abwcf.actors.persistence.page.PagePersistence.UpdateStatus
import abwcf.persistence.PageRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

import scala.util.{Failure, Success}

object PageUpdater {
  sealed trait Command
  private case class FutureSuccess(url: String) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(pageRepository: PageRepository, pageShardRegion: ActorRef[ShardingEnvelope[PageManager.Command]]): Behavior[Command | PagePersistence.UpdateCommand] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case UpdateStatus(url, status) =>
        context.pipeToSelf(pageRepository.updateStatus(url, status))({
          case Success(_) => FutureSuccess(url)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FutureSuccess(url) =>
        pageShardRegion ! ShardingEnvelope(url, PageManager.UpdateSuccess)
        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Exception while updating", throwable)
        Behaviors.same
    })
  })
}
