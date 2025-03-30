package abwcf.actors

import abwcf.PageEntity
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.util.ByteString

/**
 * Limits the crawl depth as configured.
 *
 * There should be one [[CrawlDepthLimiter]] actor per node.
 *
 * This actor is stateless.
 */
object CrawlDepthLimiter {
  sealed trait Command
  case class CheckDepth(page: PageEntity, responseBody: ByteString) extends Command

  def apply(htmlParser: ActorRef[HtmlParser.Command]): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val maxCrawlDepth = config.getInt("abwcf.crawl-depth-limiter.max-crawl-depth")

    Behaviors.receiveMessage({
      case CheckDepth(page, responseBody) =>
        if (page.crawlDepth < maxCrawlDepth) {
          htmlParser ! HtmlParser.Parse(page, responseBody)
        }

        Behaviors.same
    })
  })
}
