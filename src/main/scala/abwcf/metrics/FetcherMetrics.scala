package abwcf.metrics

import abwcf.api.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.http.scaladsl.model.HttpResponse

object FetcherMetrics {
  def apply(instrumentationScopeName: String, prefix: String, settings: CrawlerSettings, context: ActorContext[?]): FetcherMetrics = {
    new FetcherMetrics(instrumentationScopeName, prefix, settings, context)
  }
}

class FetcherMetrics private (instrumentationScopeName: String, prefix: String, settings: CrawlerSettings, context: ActorContext[?]) extends ActorMetrics(context) {
  private val meter = settings.openTelemetry.getMeter(instrumentationScopeName)

  private val requestCounter = meter.counterBuilder(s"$prefix.requests")
    .setDescription("The number of requests that have been sent.")
    .build()

  private val responseCounter = meter.counterBuilder(s"$prefix.responses")
    .setDescription("The number of responses that have been received (including responses whose response body was later discarded).")
    .build()

  private val receivedBytesCounter = meter.counterBuilder(s"$prefix.received_bytes")
    .setUnit("By") //Weird UCUM identifier for "byte".
    .setDescription("The number of response body bytes that have been received (excluding discarded response bodies).")
    .build()

  private val exceptionsCounter = meter.counterBuilder(s"$prefix.exceptions")
    .setDescription("The number of exceptions thrown while fetching resources.")
    .build()

  def addRequests(value: Long): Unit =
    requestCounter.add(value, actorAttributes)

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
