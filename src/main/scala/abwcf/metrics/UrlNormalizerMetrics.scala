package abwcf.metrics

import abwcf.actors.UrlNormalizer
import abwcf.util.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.ActorContext

object UrlNormalizerMetrics {
  private val Prefix = "abwcf.url_normalizer"

  def apply(settings: CrawlerSettings, context: ActorContext[?]): UrlNormalizerMetrics = {
    new UrlNormalizerMetrics(settings, context)
  }
}

class UrlNormalizerMetrics private (settings: CrawlerSettings, context: ActorContext[?]) extends ActorMetrics(context) {
  import UrlNormalizerMetrics.*

  private val meter = settings.openTelemetry.getMeter(UrlNormalizer.getClass.getName)

  private val processedUrlsCounter = meter.counterBuilder(s"$Prefix.processed_urls")
    .setDescription("The number of URLs that have been processed.")
    .build()

  private val exceptionsCounter = meter.counterBuilder(s"$Prefix.exceptions")
    .setDescription("The number of exceptions thrown while normalizing URLs.")
    .build()

  def addProcessedUrls(value: Long): Unit = {
    processedUrlsCounter.add(value, actorAttributes)
  }

  def addExceptions(value: Long, exception: Exception): Unit = {
    val attributes = actorAttributes.toBuilder
      .put(AttributeKeys.Exception, exception.getClass.getName)
      .build()

    exceptionsCounter.add(value, attributes)
  }
}
