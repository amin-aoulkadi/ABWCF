package abwcf.actors

import abwcf.api.CrawlerSettings
import abwcf.data.{Page, PageCandidate}
import abwcf.metrics.HtmlParserMetrics
import abwcf.services.robots_tag.RobotsMetaParsingService
import abwcf.util.RobotsUtils
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.util.ByteString
import org.jsoup.Jsoup

import scala.jdk.CollectionConverters.*
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

  def apply(urlDeduplicator: ActorRef[UrlDeduplicator.Command], settings: CrawlerSettings): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val userAgents = config.getStringList("abwcf.robots.user-agents").asScala.toSet
    val robotsMetaParser = RobotsMetaParsingService(userAgents)
    val metrics = HtmlParserMetrics(settings, context)

    Behaviors.receiveMessage({
      case Parse(page, responseBody) =>
        //Parse the HTML document:
        val document = Jsoup.parse(responseBody.utf8String, page.url)
        metrics.addParsedDocuments(1)

        //Check if the document contains any <meta name="robots"> elements that indicate that robots should not follow the links in the document:
        robotsMetaParser.reset()

        document.head
          .select("meta[name][content]") //Select all <meta> elements that have a "name" and a "content" attribute.
          .forEach(element => robotsMetaParser.parse(element.outerHtml))
        
        val canFollowLinks = RobotsUtils.canFollowLinks(robotsMetaParser.collectedDirectives)

        if (canFollowLinks) {
          //Get URLs from the document:
          val urls: List[String] = document
            .select("a[href]") //Select all <a> elements that have an "href" attribute.
            .stream()
            .map(_.absUrl("href"))
            .distinct()
            .filter(_.substring(0, 4).equalsIgnoreCase("http")) //Drop non-HTTP URLs (e.g. "mailto:someone@example.com").
            .toScala(List)

          //Send the URLs downstream:
          urls.map(PageCandidate(_, page.crawlDepth + 1)) //Important: The crawl depth increases here.
            .foreach(urlDeduplicator ! UrlDeduplicator.Deduplicate(_))

          metrics.addEmittedUrls(urls.length)
        }

        Behaviors.same
    })
  })
}
