package abwcf.metrics

import abwcf.actors.fetching.Fetcher
import abwcf.api.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.http.scaladsl.model.HttpResponse

object FetcherMetrics {
  private val Prefix = "abwcf.fetcher"

  def apply(settings: CrawlerSettings, context: ActorContext[?]): FetcherMetrics = {
    new FetcherMetrics(settings, context)
  }
}

class FetcherMetrics private (settings: CrawlerSettings, context: ActorContext[?]) extends ActorMetrics(context) {
  import FetcherMetrics.*

  private val meter = settings.openTelemetry.getMeter(Fetcher.getClass.getName)

  private val fetchedPagesCounter = meter.counterBuilder(s"$Prefix.fetched_pages")
    .setDescription("The number of pages that have been fetched.")
    .build()

  private val responseCounter = meter.counterBuilder(s"$Prefix.received_responses")
    .setDescription("The number of responses that have been received (including responses whose response body was later discarded).")
    .build()

  private val receivedBytesCounter = meter.counterBuilder(s"$Prefix.received_bytes")
    .setUnit("By") //Weird UCUM identifier for "byte".
    .setDescription("The number of response body bytes that have been received (excluding discarded response bodies).")
    .build()

  private val exceptionsCounter = meter.counterBuilder(s"$Prefix.exceptions")
    .setDescription("The number of exceptions thrown while fetching pages.")
    .build()

  def addFetchedPages(value: Long): Unit =
    fetchedPagesCounter.add(value, actorAttributes)

  def addResponse(response: HttpResponse): Unit = {
    val attributes = actorAttributesBuilder
      .put(AttributeKeys.StatusCode, response.status.intValue)
      .put(AttributeKeys.ContentType, response.entity.contentType.toString)
      .build()

    responseCounter.add(1, attributes)
  }

  def addReceivedBytes(value: Long): Unit =
    receivedBytesCounter.add(value, actorAttributes)

  def addException(exception: Throwable): Unit = {
    val attributes = actorAttributesBuilder
      .put(AttributeKeys.Exception, exception.getClass.getName)
      .build()

    exceptionsCounter.add(1, attributes)
  }
}
