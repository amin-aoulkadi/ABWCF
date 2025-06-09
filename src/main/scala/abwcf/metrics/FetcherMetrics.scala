package abwcf.metrics

import abwcf.api.CrawlerSettings
import io.opentelemetry.api.common.Attributes
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.http.scaladsl.model.HttpResponse

object FetcherMetrics {
  def apply(instrumentationScopeName: String, metricNamePrefix: String, settings: CrawlerSettings, context: ActorContext[?]): FetcherMetrics = {
    new FetcherMetrics(instrumentationScopeName, metricNamePrefix, settings, context)
  }
}

class FetcherMetrics private (instrumentationScopeName: String, prefix: String, settings: CrawlerSettings, context: ActorContext[?]) {
  private val meter = settings.openTelemetry.getMeter(instrumentationScopeName)
  
  private val baseAttributes = Attributes.of(
    //The actor path is not included to avoid reaching the cardinality limit.
    AttributeKeys.ActorSystemAddress, context.system.address.toString
  )

  private val baseAttributesBuilder = baseAttributes.toBuilder

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
    requestCounter.add(value, baseAttributes)

  def addResponse(response: HttpResponse): Unit = {
    val attributes = baseAttributesBuilder
      .put(AttributeKeys.StatusCode, response.status.intValue)
      .put(AttributeKeys.ContentType, response.entity.contentType.toString)
      .build()

    responseCounter.add(1, attributes)
  }

  def addReceivedBytes(value: Long): Unit =
    receivedBytesCounter.add(value, baseAttributes)

  def addException(exception: Throwable): Unit = {
    val attributes = baseAttributesBuilder
      .put(AttributeKeys.Exception, exception.getClass.getName)
      .build()

    exceptionsCounter.add(1, attributes)
  }
}
