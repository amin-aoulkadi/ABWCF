package abwcf.actors

import abwcf.data.{FetchResponse, Page}
import abwcf.util.CrawlerSettings
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
  case class ProcessSuccess(page: Page, response: FetchResponse) extends Command
  case class ProcessRedirect(page: Page, statusCode: StatusCode, redirectTo: Option[String]) extends Command
  case class ProcessError(page: Page, statusCode: StatusCode) extends Command

  def apply(settings: CrawlerSettings, pageShardRegion: ActorRef[ShardingEnvelope[PageManager.Command]]): Behavior[Command] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case ProcessSuccess(page, response) =>
        settings.userCode.onFetchSuccess(page, response, context)
        pageShardRegion ! ShardingEnvelope(page.url, PageManager.Success) //Tell the PageManager that the page has been processed.
        Behaviors.same

      case ProcessRedirect(page, statusCode, redirectTo) =>
        settings.userCode.onFetchRedirect(page, statusCode, redirectTo, context)
        pageShardRegion ! ShardingEnvelope(page.url, PageManager.Redirect) //Tell the PageManager that the page has been processed.
        Behaviors.same

      case ProcessError(page, statusCode) =>
        settings.userCode.onFetchError(page, statusCode, context)
        pageShardRegion ! ShardingEnvelope(page.url, PageManager.Error) //Tell the PageManager that the page has been processed.
        Behaviors.same
    })
  })
}
