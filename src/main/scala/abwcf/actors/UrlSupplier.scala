package abwcf.actors

import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.util.Timeout

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*
import scala.util.{Failure, Success}

/**
 * Requests URLs for a [[Fetcher]] from a [[HostQueueRouter]].
 *
 * URLs are requested in bursts. There is a delay between bursts to avoid fruitless request-reply cycles when all or most [[HostQueue]]s are unavailable. Without this delay, such request-reply cycles would result in significant CPU usage. The delay increases after every unsuccessful burst and decreases after every successful burst.
 *
 * There should be one [[UrlSupplier]] actor per [[Fetcher]] actor.
 *
 * This actor is stateful.
 */
object UrlSupplier {
  sealed trait Command
  case object RequestNext extends Command
  private case object AskFailure extends Command

  private type CombinedCommand = Command | HostQueue.Reply

  def apply(fetcher: ActorRef[Fetcher.Command], hostQueueRouter: ActorRef[HostQueue.Command]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    Behaviors.withTimers(timers => {
      new UrlSupplier(fetcher, hostQueueRouter, context, timers).requestBurst(0, 0)
    })
  }).narrow
}

private class UrlSupplier private(fetcher: ActorRef[Fetcher.Command],
                                  hostQueueRouter: ActorRef[HostQueue.Command],
                                  context: ActorContext[UrlSupplier.CombinedCommand],
                                  timers: TimerScheduler[UrlSupplier.CombinedCommand]) {
  import UrlSupplier.*

  private val config = context.system.settings.config
  private val askTimeout = config.getDuration("abwcf.url-supplier.ask-timeout").toScala
  private val burstLength = config.getInt("abwcf.url-supplier.burst-length")
  private val minDelay = config.getInt("abwcf.url-supplier.min-delay")
  private val maxDelay = config.getInt("abwcf.url-supplier.max-delay")
  private val delayStep = config.getInt("abwcf.url-supplier.delay-step")

  /**
   * Asks the HostQueueRouter for a URL.
   */
  private def askForUrl(): Unit = {
    context.ask(hostQueueRouter, HostQueue.GetHead.apply)({
      case Success(reply) => reply
      case Failure(_) => AskFailure
    })(askTimeout)
  }

  private def requestBurst(burstCounter: Int, delay: Int): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case RequestNext =>
      askForUrl()
      requestBurst(1, delay) //Reset the burst counter.

    case HostQueue.Head(page) =>
      fetcher ! Fetcher.Fetch(page)
      requestBurst(burstCounter, math.max(minDelay, delay - delayStep)) //Decrease the delay.

    case HostQueue.Unavailable | AskFailure if burstCounter < burstLength =>
      askForUrl()
      requestBurst(burstCounter + 1, delay)

    case HostQueue.Unavailable | AskFailure =>
      timers.startSingleTimer(RequestNext, FiniteDuration(delay, TimeUnit.MILLISECONDS)) //Back off before starting the next request burst.
      requestBurst(burstCounter, math.min(maxDelay, delay + delayStep)) //Increase the delay.
  })
}
