package abwcf.actors

import abwcf.api.FetchResult.*
import abwcf.api.{CrawlerSettings, FetchResult}
import abwcf.data.{Page, PageStatus}
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

/**
 * Executes user-defined code to process crawled pages.
 *
 * There should be one [[FetchResultConsumer]] actor per node.
 *
 * This actor is stateless.
 */
object FetchResultConsumer {
  def apply(settings: CrawlerSettings): Behavior[FetchResult.Command] = Behaviors.setup(context => {
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
