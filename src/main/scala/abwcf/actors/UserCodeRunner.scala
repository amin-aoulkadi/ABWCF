package abwcf.actors

import abwcf.FetchResponse
import abwcf.actors.persistence.PagePersistenceManager
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.http.scaladsl.model.StatusCode

/**
 * Executes user-defined code to process crawled pages.
 *
 * There should be one [[UserCodeRunner]] actor per node.
 *
 * This actor is stateless.
 */
object UserCodeRunner {
  sealed trait Command
  case class ProcessSuccess(url: String, response: FetchResponse) extends Command
  case class ProcessRedirect(url: String, statusCode: StatusCode, redirectTo: Option[String]) extends Command
  case class ProcessError(url: String, statusCode: StatusCode) extends Command

  def apply(pagePersistenceManager: ActorRef[PagePersistenceManager.Command]): Behavior[Command] = Behaviors.setup(context => {
    val pageShardRegion = Page.getShardRegion(context.system, pagePersistenceManager)

    Behaviors.receiveMessage({
      case ProcessSuccess(url, response) =>
        //TODO: Provide an API to inject user-defined code.
        context.log.info("Processing page {} ({}, {} bytes)", url, response.status, response.body.length)
        pageShardRegion ! ShardingEnvelope(url, Page.Success) //Tell the Page that it has been processed.
        Behaviors.same

      case ProcessRedirect(url, statusCode, redirectTo) =>
        context.log.info("Processing redirect from {} ({}, redirection to {})", url, statusCode, redirectTo)
        pageShardRegion ! ShardingEnvelope(url, Page.Redirect) //Tell the Page that it has been processed.
        Behaviors.same

      case ProcessError(url, statusCode) =>
        context.log.info("Processing error from {} ({})", url, statusCode)
        pageShardRegion ! ShardingEnvelope(url, Page.Error)
        Behaviors.same
    })
  })
}
