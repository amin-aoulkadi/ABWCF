package abwcf.actors

import abwcf.data.{Page, PageCandidate}
import abwcf.metrics.HtmlParserMetrics
import abwcf.util.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.util.ByteString
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.jdk.StreamConverters.*

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

  def apply(urlNormalizer: ActorRef[UrlNormalizer.Command], settings: CrawlerSettings): Behavior[Command] = Behaviors.setup(context => {
    val metrics = HtmlParserMetrics(settings, context)

    Behaviors.receiveMessage({
      case Parse(page, responseBody) =>
        //Parse the HTML document:
        val document = Jsoup.parse(responseBody.utf8String, page.url)
        metrics.addParsedDocuments(1)

        if (canFollowLinks(document)) {
          //Get URLs from the document:
          val urls: List[String] = document
            .select("a[href]") //Select all <a> elements that have an href attribute.
            .stream()
            .map(_.absUrl("href"))
            .distinct()
            .filter(_.substring(0, 4).equalsIgnoreCase("http")) //Drop non-HTTP URLs (e.g. "mailto:someone@example.com").
            .toScala(List)

          //Send the URLs to the UrlNormalizer:
          urls.map(PageCandidate(_, page.crawlDepth + 1)) //Important: The crawl depth increases here.
            .foreach(urlNormalizer ! UrlNormalizer.Normalize(_))

          metrics.addEmittedUrls(urls.length)
        }

        Behaviors.same
    })
  })

  /**
   * Checks if the document contains any `<meta name="robots" content="...">` elements that indicate that robots should not follow the links in the document.
   *
   * @return `true` if the links in the document can be followed, otherwise `false`
   *
   * @note There is no official standard or specification for `<meta name="robots" content="...">` elements, so each vendor supports a different set of rules.
   * @see
   *      - [[https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/meta/name MDN: Standard metadata names]]
   *      - [[https://developers.google.com/search/docs/crawling-indexing/robots-meta-tag Google: Robots Meta Tags Specifications]]
   */
  private def canFollowLinks(document: Document): Boolean = {
    document.select("meta[name=robots][content]") //Select all <meta name="robots" content="..."> elements.
      .stream()
      .map(_.attr("content")) //"noindex, nofollow"
      .flatMap(_.split(',').asJavaSeqStream) //"noindex", " nofollow"
      .map(_.trim) //"noindex", "nofollow"
      .noneMatch(_.equalsIgnoreCase("nofollow"))
  }
}
