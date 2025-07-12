package abwcf.actors

import abwcf.data.{FetchResponse, Page, PageStatus}
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding
import org.apache.pekko.http.scaladsl.model.StatusCode

/**
 * Executes user-defined code to process crawled pages.
 *
 * There should be one [[FetchResultConsumer]] actor per node.
 *
 * This actor is stateless, but users may also implement stateful variants.
 */
object FetchResultConsumer {
  sealed trait Command
  case class Success(page: Page, response: FetchResponse) extends Command
  case class Redirect(page: Page, statusCode: StatusCode, redirectTo: Option[String]) extends Command
  case class Error(page: Page, statusCode: StatusCode) extends Command
  case class LengthLimitExceeded(page: Page, response: FetchResponse) extends Command
  
  def apply(): Behavior[Command] = Behaviors.setup(context => {
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
        notifyPageManager(page)
        Behaviors.same

      case Redirect(page, statusCode, redirectTo) =>
        notifyPageManager(page)
        Behaviors.same

      case Error(page, statusCode) =>
        notifyPageManager(page)
        Behaviors.same

      case LengthLimitExceeded(page, response) =>
        notifyPageManager(page)
        Behaviors.same
    })
  })
}
