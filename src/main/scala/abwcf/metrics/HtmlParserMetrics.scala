package abwcf.metrics

import abwcf.actors.HtmlParser
import abwcf.api.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.ActorContext

object HtmlParserMetrics {
  private val Prefix = "abwcf.html_parser"

  def apply(settings: CrawlerSettings, context: ActorContext[?]): HtmlParserMetrics = {
    new HtmlParserMetrics(settings, context)
  }
}

class HtmlParserMetrics private (settings: CrawlerSettings, context: ActorContext[?]) extends ActorMetrics(context) {
  import HtmlParserMetrics.*

  private val meter = settings.openTelemetry.getMeter(HtmlParser.getClass.getName)

  private val parsedDocumentsCounter = meter.counterBuilder(s"$Prefix.parsed_documents")
    .setDescription("The number of HTML documents that have been parsed.")
    .build()
  
  private val emittedUrlsCounter = meter.counterBuilder(s"$Prefix.emitted_urls")
    .setDescription("The number of URLs that have been found while parsing.")
    .build()

  def addParsedDocuments(value: Long): Unit =
    parsedDocumentsCounter.add(value, actorAttributes)
    
  def addEmittedUrls(value: Long): Unit =
    emittedUrlsCounter.add(value, actorAttributes)
}
