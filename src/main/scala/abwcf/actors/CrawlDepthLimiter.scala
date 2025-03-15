package abwcf.actors

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
  case class CheckDepth(url: String, responseBody: ByteString) extends Command

  def apply(htmlParser: ActorRef[HtmlParser.Command]): Behavior[Command] = Behaviors.receiveMessage({
    case CheckDepth(url, responseBody) =>
      //TODO: Implement crawl depth limiting.
      htmlParser ! HtmlParser.Parse(url, responseBody)
      Behaviors.same
  })
}
