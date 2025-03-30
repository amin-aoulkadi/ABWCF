package abwcf.actors

import abwcf.actors.persistence.PagePersistenceManager
import abwcf.{FetchResponse, PageEntity}
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
  case class ProcessSuccess(page: PageEntity, response: FetchResponse) extends Command
  case class ProcessRedirect(page: PageEntity, statusCode: StatusCode, redirectTo: Option[String]) extends Command
  case class ProcessError(page: PageEntity, statusCode: StatusCode) extends Command

  def apply(pagePersistenceManager: ActorRef[PagePersistenceManager.Command]): Behavior[Command] = Behaviors.setup(context => {
    val pageShardRegion = Page.getShardRegion(context.system, pagePersistenceManager)

    Behaviors.receiveMessage({
      case ProcessSuccess(page, response) =>
        //TODO: Provide an API to inject user-defined code.
        context.log.info("Processing page {} ({}, {} bytes)", page.url, response.status, response.body.length)
        pageShardRegion ! ShardingEnvelope(page.url, Page.Success) //Tell the Page that it has been processed.
        Behaviors.same

      case ProcessRedirect(page, statusCode, redirectTo) =>
        context.log.info("Processing redirect from {} ({}, redirection to {})", page.url, statusCode, redirectTo)
        pageShardRegion ! ShardingEnvelope(page.url, Page.Redirect) //Tell the Page that it has been processed.
        Behaviors.same

      case ProcessError(page, statusCode) =>
        context.log.info("Processing error from {} ({})", page.url, statusCode)
        pageShardRegion ! ShardingEnvelope(page.url, Page.Error)
        Behaviors.same
    })
  })
}
