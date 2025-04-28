package abwcf.actors

import abwcf.data.{FetchResponse, Page}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

/**
 * Limits the crawl depth as configured.
 *
 * There should be one [[CrawlDepthLimiter]] actor per node.
 *
 * This actor is stateless.
 */
object CrawlDepthLimiter {
  sealed trait Command
  case class CheckDepth(page: Page, response: FetchResponse) extends Command

  def apply(robotsHeaderFilter: ActorRef[RobotsHeaderFilter.Command]): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val maxCrawlDepth = config.getInt("abwcf.actors.crawl-depth-limiter.max-crawl-depth")

    Behaviors.receiveMessage({
      case CheckDepth(page, response) =>
        if (page.crawlDepth < maxCrawlDepth) {
          robotsHeaderFilter ! RobotsHeaderFilter.Filter(page, response)
        }

        Behaviors.same
    })
  })
}
