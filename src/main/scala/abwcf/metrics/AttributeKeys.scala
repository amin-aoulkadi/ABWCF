package abwcf.metrics

import io.opentelemetry.api.common.AttributeKey

object AttributeKeys {
  val ActorPath: AttributeKey[String] = AttributeKey.stringKey("actor.path")
  val Exception: AttributeKey[String] = AttributeKey.stringKey("exception")
}
