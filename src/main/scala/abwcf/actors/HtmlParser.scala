package abwcf.actors

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.util.ByteString
import org.jsoup.Jsoup

import scala.jdk.StreamConverters.StreamHasToScala

/**
 * Retrieves HTTP URLs from HTML documents.
 *
 * There should be one [[HtmlParser]] actor per node.
 *
 * This actor is stateless.
 */
object HtmlParser {
  sealed trait Command
  case class Parse(url: String, crawlDepth: Int, responseBody: ByteString) extends Command

  def apply(urlNormalizer: ActorRef[UrlNormalizer.Command]): Behavior[Command] = Behaviors.receiveMessage({
    case Parse(url, crawlDepth, responseBody) =>
      val urls: List[String] = Jsoup.parse(responseBody.utf8String, url)
        .select("a[href]") //Select all <a> elements that have an href attribute.
        .stream()
        .map(_.absUrl("href"))
        .distinct()
        .filter(_.startsWith("http")) //Drop non-HTTP URLs (e.g. "mailto:someone@example.com").
        .toScala(List)

      urls.foreach(urlNormalizer ! UrlNormalizer.Normalize(_, crawlDepth + 1)) //Important: The crawl depth increases here.
      //TODO: Maybe debounce discovered URLs to eliminate duplicates across multiple responses (e.g. via a custom mailbox for the downstream actor)?
      Behaviors.same
  })
}
