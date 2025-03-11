package abwcf.actors

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.util.Timeout

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}

object Fetcher {
  sealed trait Command
  private case object AskFailure extends Command

  private type CombinedMessages = Command | HostQueue.Reply

  def apply(hostQueueRouter: ActorRef[HostQueue.Command]): Behavior[Command] = Behaviors.setup[CombinedMessages](context => {
    val getHeadTimeout: Timeout = 3 seconds //TODO: Add to config.

    def requestUrl(): Unit = context.ask(hostQueueRouter, HostQueue.GetHead.apply) {
      case Success(reply: HostQueue.Reply) => reply
      case Failure(_) => AskFailure
    }(getHeadTimeout)

    requestUrl()

    Behaviors.receiveMessage({
      case HostQueue.Head(url) =>
        context.log.info("Fetching {}", url)
        requestUrl()
        Behaviors.same

      case HostQueue.Unavailable | AskFailure =>
        requestUrl()
        Behaviors.same
    })
  }).narrow
}
