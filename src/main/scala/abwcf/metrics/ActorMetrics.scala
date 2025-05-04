package abwcf.metrics

import io.opentelemetry.api.common.Attributes
import org.apache.pekko.actor.typed.scaladsl.ActorContext

trait ActorMetrics(context: ActorContext[?]) {
  /**
   * OpenTelemetry attributes that every actor-specific metric should have.
   */
  protected val actorAttributes: Attributes = Attributes.of(
    //Could also use context.self.path.toStringWithAddress(context.system.address) to get "pekko://crawler@127.0.0.1:7354/user/my-parent/my-actor".
    AttributeKeys.ActorPath, context.self.path.toStringWithoutAddress,
    AttributeKeys.ActorSystemAddress, context.system.address.toString
  )
}
