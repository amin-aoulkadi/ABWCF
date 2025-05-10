package abwcf.metrics

import io.opentelemetry.api.common.AttributeKey

object AttributeKeys {
  val ActorPath: AttributeKey[String] = AttributeKey.stringKey("actor.path")
  val ActorSystemAddress: AttributeKey[String] = AttributeKey.stringKey("actor_system.address")
  val Exception: AttributeKey[String] = AttributeKey.stringKey("exception")

  //OpenTelemetry specification for HTTP attributes: https://opentelemetry.io/docs/specs/semconv/attributes-registry/http/
  val ContentType: AttributeKey[java.util.List[String]] = AttributeKey.stringArrayKey("http.response.header.content-type")
  val StatusCode: AttributeKey[java.lang.Long] = AttributeKey.longKey("http.response.status_code")
}
