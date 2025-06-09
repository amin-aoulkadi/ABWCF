package abwcf.actors.metrics

import abwcf.api.CrawlerSettings
import abwcf.metrics.AttributeKeys
import io.opentelemetry.api.common.Attributes
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.model.HttpResponse

import scala.collection.mutable

object FetcherMetricsAggregatorAsync {
  sealed trait Command
  case class AddRequests(value: Long) extends Command
  case class AddResponse(response: HttpResponse) extends Command
  case class AddReceivedBytes(value: Long) extends Command
  case class AddException(exception: Throwable) extends Command

  def apply(instrumentationScopeName: String, prefix: String, settings: CrawlerSettings): Behavior[Command] = Behaviors.setup(context => {
    val meter = settings.openTelemetry.getMeter(instrumentationScopeName)

    val baseAttributes = Attributes.of(
      AttributeKeys.ActorPath, context.self.path.toStringWithoutAddress,
      AttributeKeys.ActorSystemAddress, context.system.address.toString
    )

    val baseAttributesBuilder = baseAttributes.toBuilder

    var requests = 0L
    var receivedBytes = 0L
    val responses = mutable.Map.empty[Attributes, Long]
    val exceptions = mutable.Map.empty[Attributes, Long]

    val requestCounter = meter.counterBuilder(s"$prefix.requests")
      .setDescription("The number of requests that have been sent.")
      .buildWithCallback(_.record(requests, baseAttributes))

    val responseCounter = meter.counterBuilder(s"$prefix.responses")
      .setDescription("The number of responses that have been received (including responses whose response body was later discarded).")
      .buildWithCallback(observableMeasurement => responses.foreach((attributes, value) => observableMeasurement.record(value, attributes)))

    val receivedBytesCounter = meter.counterBuilder(s"$prefix.received_bytes")
      .setUnit("By") //Weird UCUM identifier for "byte".
      .setDescription("The number of response body bytes that have been received (excluding discarded response bodies).")
      .buildWithCallback(_.record(receivedBytes, baseAttributes))

    val exceptionsCounter = meter.counterBuilder(s"$prefix.exceptions")
      .setDescription("The number of exceptions thrown while fetching resources.")
      .buildWithCallback(observableMeasurement => exceptions.foreach((attributes, value) => observableMeasurement.record(value, attributes)))

    Behaviors.receiveMessage({
      case AddRequests(value) =>
        requests = requests + value
        Behaviors.same

      case AddResponse(response) =>
        val attributes = baseAttributesBuilder
          .put(AttributeKeys.StatusCode, response.status.intValue)
          .put(AttributeKeys.ContentType, response.entity.contentType.toString)
          .build()

        responses.updateWith(attributes)({
          case Some(value) => Some(value + 1)
          case None => Some(1)
        })

        Behaviors.same

      case AddReceivedBytes(value) =>
        receivedBytes = receivedBytes + value
        Behaviors.same

      case AddException(exception) =>
        val attributes = baseAttributesBuilder
          .put(AttributeKeys.Exception, exception.getClass.getName)
          .build()

        exceptions.updateWith(attributes)({
          case Some(value) => Some(value + 1)
          case None => Some(1)
        })

        Behaviors.same
    })
  })
}
