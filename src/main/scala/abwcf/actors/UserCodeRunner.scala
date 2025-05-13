package abwcf.actors

import abwcf.data.{FetchResponse, Page, PageStatus}
import abwcf.util.CrawlerSettings
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding
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
  case class Success(page: Page, response: FetchResponse) extends Command
  case class Redirect(page: Page, statusCode: StatusCode, redirectTo: Option[String]) extends Command
  case class Error(page: Page, statusCode: StatusCode) extends Command
  case class LengthLimitExceeded(page: Page, response: FetchResponse) extends Command

  def apply(settings: CrawlerSettings): Behavior[Command] = Behaviors.setup(context => {
    val sharding = ClusterSharding(context.system)

    /**
     * Notifies the corresponding [[PageManager]] that the page has been processed.
     */
    def notifyPageManager(page: Page): Unit = {
      val pageManager = sharding.entityRefFor(PageManager.TypeKey, page.url)
      pageManager ! PageManager.SetStatus(PageStatus.Processed)
    }

    Behaviors.receiveMessage({
      case Success(page, response) =>
        settings.userCode.onFetchSuccess(page, response, context)
        notifyPageManager(page)
        Behaviors.same

      case Redirect(page, statusCode, redirectTo) =>
        settings.userCode.onFetchRedirect(page, statusCode, redirectTo, context)
        notifyPageManager(page)
        Behaviors.same

      case Error(page, statusCode) => //TODO: Maybe let the user code decide whether to try again later or not.
        settings.userCode.onFetchError(page, statusCode, context)
        notifyPageManager(page)
        Behaviors.same

      case LengthLimitExceeded(page, response) =>
        settings.userCode.onLengthLimitExceeded(page, response, context)
        notifyPageManager(page)
        Behaviors.same
    })
  })
}
