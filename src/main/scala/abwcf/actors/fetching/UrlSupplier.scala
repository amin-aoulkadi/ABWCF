package abwcf.actors.fetching

import abwcf.actors.{HostQueue, HostQueueRouter}
import abwcf.util.TimeUtils
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.util.Timeout

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*
import scala.util.{Failure, Success}

/**
 * Requests URLs for a [[Fetcher]] from a [[HostQueueRouter]].
 *
 * URLs are requested in bursts. There is a delay between bursts to avoid fruitless request-reply cycles when all or most [[HostQueue]]s are unavailable. Without this delay, such request-reply cycles would result in significant CPU usage.
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
      new UrlSupplier(fetcher, hostQueueRouter, context, timers).requestBurst(0, Instant.MAX)
    })
  }).narrow
}

private class UrlSupplier private(fetcher: ActorRef[Fetcher.Command],
                                  hostQueueRouter: ActorRef[HostQueue.Command],
                                  context: ActorContext[UrlSupplier.CombinedCommand],
                                  timers: TimerScheduler[UrlSupplier.CombinedCommand]) {
  import UrlSupplier.*

  private val config = context.system.settings.config
  private val askTimeout = config.getDuration("abwcf.actors.url-supplier.ask-timeout").toScala
  private val burstLength = config.getInt("abwcf.actors.url-supplier.burst-length")
  private val minDelay = FiniteDuration(config.getInt("abwcf.actors.url-supplier.min-delay"), TimeUnit.MILLISECONDS)
  private val maxDelay = FiniteDuration(config.getInt("abwcf.actors.url-supplier.max-delay"), TimeUnit.MILLISECONDS)

  /**
   * Asks the HostQueueRouter for a URL.
   */
  private def askForUrl(): Unit = {
    context.ask(hostQueueRouter, HostQueue.GetHead.apply)({
      case Success(reply) => reply
      case Failure(_) => AskFailure
    })(using askTimeout)
  }

  /**
   * Backs off before starting a new request burst.
   */
  private def backOff(delayEnd: Instant): Unit = {
    //Determine the delay:
    var delay = TimeUtils.asFiniteDuration(Instant.now.until(delayEnd)) //Using TimeUtils.asFiniteDuration() to avoid an exception if delayEnd is Instant.MAX.
    delay = delay.max(minDelay) //Ensure that delay ≥ minDelay.
    delay = delay.min(maxDelay) //Ensure that delay ≤ maxDelay.

    context.log.debug("Backing off for {} ms", delay.toMillis)
    timers.startSingleTimer(RequestNext, delay)
  }

  private def requestBurst(burstCounter: Int, smallestSeenInstant: Instant): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case RequestNext =>
      //Start a new request burst:
      askForUrl()
      requestBurst(1, Instant.MAX) //Reset the burst counter and the smallest seen instant.

    case HostQueue.Head(page) =>
      fetcher ! Fetcher.Fetch(page)
      Behaviors.same

    case HostQueue.Unavailable(until) if burstCounter < burstLength =>
      //Continue the current request burst:
      val smallerInstant = if until.isBefore(smallestSeenInstant) then until else smallestSeenInstant
      askForUrl()
      requestBurst(burstCounter + 1, smallerInstant)

    case HostQueue.Unavailable(until) =>
      //Back off and start a new request burst:
      val smallerInstant = if until.isBefore(smallestSeenInstant) then until else smallestSeenInstant
      backOff(smallerInstant)
      Behaviors.same

    case AskFailure if burstCounter < burstLength =>
      //Continue the current request burst:
      askForUrl()
      requestBurst(burstCounter + 1, smallestSeenInstant)

    case AskFailure =>
      //Back off and start a new request burst:
      backOff(smallestSeenInstant)
      Behaviors.same
  })
}
