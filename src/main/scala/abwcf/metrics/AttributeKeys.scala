package abwcf.metrics

import io.opentelemetry.api.common.AttributeKey

object AttributeKeys {
  val ActorPath: AttributeKey[String] = AttributeKey.stringKey("actor.path")
  val ActorSystemAddress: AttributeKey[String] = AttributeKey.stringKey("actor_system.address")
  val Exception: AttributeKey[String] = AttributeKey.stringKey("exception")
}
