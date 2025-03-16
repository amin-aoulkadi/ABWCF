package abwcf.actors

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.util.ByteString

/**
 * Executes user-defined code to process crawled pages.
 *
 * There should be one [[UserCodeRunner]] actor per node.
 *
 * This actor is stateless and registered with the receptionist.
 */
object UserCodeRunner {
  val UCRServiceKey: ServiceKey[Command] = ServiceKey("UserCodeRunner")

  sealed trait Command
  case class ProcessPage(url: String, response: HttpResponse, responseBody: ByteString) extends Command

  def apply(): Behavior[Command] = Behaviors.setup(context => {
    context.system.receptionist ! Receptionist.Register(UCRServiceKey, context.self)

    Behaviors.receiveMessage({
      case ProcessPage(url, response, responseBody) =>
        //TODO: Provide an API to inject user-defined code.
        context.log.info("Processing {} ({}, {} bytes)", url, response.status, responseBody.length)
        Behaviors.same
    })
  })
}
