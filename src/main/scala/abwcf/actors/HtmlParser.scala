package abwcf.actors

import abwcf.data.{Page, PageCandidate}
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
  case class Parse(page: Page, responseBody: ByteString) extends Command

  def apply(urlNormalizer: ActorRef[UrlNormalizer.Command]): Behavior[Command] = Behaviors.receiveMessage({
    case Parse(page, responseBody) =>
      //Parse the HTML document and get URLs:
      val urls: List[String] = Jsoup.parse(responseBody.utf8String, page.url)
        .select("a[href]") //Select all <a> elements that have an href attribute.
        .stream()
        .map(_.absUrl("href"))
        .distinct()
        .filter(_.startsWith("http")) //Drop non-HTTP URLs (e.g. "mailto:someone@example.com").
        .toScala(List)

      //Send the URLs to the UrlNormalizer:
      urls.map(PageCandidate(_, page.crawlDepth + 1)) //Important: The crawl depth increases here.
        .foreach(urlNormalizer ! UrlNormalizer.Normalize(_))

      //TODO: Maybe debounce discovered URLs to eliminate duplicates across multiple responses (e.g. via a custom mailbox for the downstream actor)?
      Behaviors.same
  })
}
