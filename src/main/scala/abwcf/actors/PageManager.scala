package abwcf.actors

import abwcf.actors.persistence.PagePersistenceManager
import abwcf.{FetchResponse, PageEntity}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.http.scaladsl.model.StatusCode

/**
 * Orchestrates certain interactions with [[Page]] actors.
 *
 * There should be one [[PageManager]] actor per node.
 *
 * This actor is stateless.
 */
object PageManager { //TODO: There's not a lot of management here. Maybe rename to PageCoordinator or PageOrchestrator.
  sealed trait Command
  case class Discover(page: PageEntity) extends Command
  case class FetchSuccess(page: PageEntity, response: FetchResponse) extends Command
  case class FetchRedirect(page: PageEntity, statusCode: StatusCode, redirectTo: Option[String]) extends Command
  case class FetchError(page: PageEntity, statusCode: StatusCode) extends Command

  def apply(pagePersistenceManager: ActorRef[PagePersistenceManager.Command], userCodeRunner: ActorRef[UserCodeRunner.Command]): Behavior[Command] = Behaviors.setup(context => {
    val pageShardRegion = Page.getShardRegion(context.system, pagePersistenceManager)

    Behaviors.receiveMessage({
      case Discover(page) => //TODO: Add database lookup (with a small cache).
        pageShardRegion ! ShardingEnvelope(page.url, Page.Discover(page.crawlDepth))
        Behaviors.same

      case FetchSuccess(page, response) =>
        userCodeRunner ! UserCodeRunner.ProcessSuccess(page, response)
        Behaviors.same

      case FetchRedirect(page, statusCode, redirectTo) =>
        userCodeRunner ! UserCodeRunner.ProcessRedirect(page, statusCode, redirectTo)
        Behaviors.same

      case FetchError(page, statusCode) => //TODO: Retry fetching later. Maybe let the user code decide whether to retry or not.
        userCodeRunner ! UserCodeRunner.ProcessError(page, statusCode)
        Behaviors.same
    })
  })
}
