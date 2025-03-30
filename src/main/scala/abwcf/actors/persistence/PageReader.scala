package abwcf.actors.persistence

import abwcf.actors.Page
import abwcf.persistence.PageRepository
import abwcf.{PageEntity, PageStatus}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

import scala.util.{Failure, Success}

object PageReader {
  sealed trait Command
  case class Recover(url: String) extends Command
  case class FindByStatus(status: PageStatus, limit: Int, replyTo: ActorRef[ResultSeq]) extends Command
  private case class RecoverSuccess(result: Option[PageEntity], replyToUrl: String) extends Command
  private case class FindByStatusSuccess(result: Seq[PageEntity], replyTo: ActorRef[ResultSeq]) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  sealed trait Reply
  case class ResultSeq(result: Seq[PageEntity]) extends Reply

  def apply(pageRepository: PageRepository, pageShardRegion: ActorRef[ShardingEnvelope[Page.Command]]): Behavior[Command] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case Recover(url) =>
        context.pipeToSelf(pageRepository.findByUrl(url))({
          case Success(result) => RecoverSuccess(result, url)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FindByStatus(status, limit, replyTo) =>
        context.pipeToSelf(pageRepository.findByStatus(status, limit))({
          case Success(result) => FindByStatusSuccess(result, replyTo)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case RecoverSuccess(result, replyToUrl) =>
        pageShardRegion ! ShardingEnvelope(replyToUrl, Page.RecoveryResult(result))
        Behaviors.same

      case FindByStatusSuccess(result, replyTo) =>
        replyTo ! ResultSeq(result)
        Behaviors.same

      case FutureFailure(throwable) =>
        context.log.error("Error while inserting", throwable)
        Behaviors.same
    })
  })
}
