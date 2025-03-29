package abwcf.actors

import abwcf.FetchResponse
import abwcf.actors.persistence.PagePersistence
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
  case class Discover(url: String, crawlDepth: Int) extends Command //TODO: Rename to Discover.
  case class FetchSuccess(url: String, response: FetchResponse) extends Command
  case class FetchRedirect(url: String, statusCode: StatusCode, redirectTo: Option[String]) extends Command
  case class FetchError(url: String, statusCode: StatusCode) extends Command

  def apply(pagePersistence: ActorRef[PagePersistence.Command], userCodeRunner: ActorRef[UserCodeRunner.Command]): Behavior[Command] = Behaviors.setup(context => {
    val hostQueueShardRegion = HostQueue.getShardRegion(context.system)
    val pageShardRegion = Page.getShardRegion(context.system, pagePersistence)

    Behaviors.receiveMessage({
      case Discover(url, crawlDepth) => //TODO: Add database lookup (with a small cache).
        pageShardRegion ! ShardingEnvelope(url, Page.Discover(crawlDepth))
        Behaviors.same

      case FetchSuccess(url, response) =>
        userCodeRunner ! UserCodeRunner.ProcessSuccess(url, response)
        Behaviors.same

      case FetchRedirect(url, statusCode, redirectTo) =>
        userCodeRunner ! UserCodeRunner.ProcessRedirect(url, statusCode, redirectTo)
        Behaviors.same

      case FetchError(url, statusCode) => //TODO: Retry fetching later. Maybe let the user code decide whether to retry or not.
        userCodeRunner ! UserCodeRunner.ProcessError(url, statusCode)
        Behaviors.same
    })
  })
}
