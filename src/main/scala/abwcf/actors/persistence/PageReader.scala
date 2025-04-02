package abwcf.actors.persistence

import abwcf.PageEntity
import abwcf.actors.PageManager
import abwcf.actors.persistence.PagePersistence.{FindByStatus, Recover, ResultSeq}
import abwcf.persistence.PageRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

import scala.util.{Failure, Success}

object PageReader {
  sealed trait Command
  private case class RecoverSuccess(result: Option[PageEntity], replyToUrl: String) extends Command
  private case class FindByStatusSuccess(result: Seq[PageEntity], replyTo: ActorRef[ResultSeq]) extends Command
  private case class FutureFailure(throwable: Throwable) extends Command

  def apply(pageRepository: PageRepository, pageShardRegion: ActorRef[ShardingEnvelope[PageManager.Command]]): Behavior[Command | PagePersistence.ReadCommand] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case Recover(url) =>
        context.pipeToSelf(pageRepository.findByUrl(url))({
          case Success(result) => RecoverSuccess(result, url)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case FindByStatus(status, limit, replyTo) =>
        context.pipeToSelf(pageRepository.findByStatusOrderByCrawlPriorityDesc(status, limit))({
          case Success(result) => FindByStatusSuccess(result, replyTo)
          case Failure(throwable) => FutureFailure(throwable)
        })
        Behaviors.same

      case RecoverSuccess(result, replyToUrl) =>
        pageShardRegion ! ShardingEnvelope(replyToUrl, PageManager.RecoveryResult(result))
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
