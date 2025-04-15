package abwcf.actors.fetching

import abwcf.actors.*
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}

import scala.collection.mutable
import scala.jdk.DurationConverters.*

/**
 * Manages [[Fetcher]] actors. Uses a [[ManagementDataAggregator]] to collect the required data.
 *
 * There should be one [[FetcherManager]] actor per node.
 *
 * This actor is stateful.
 */
object FetcherManager {
  sealed trait Command
  private case object ScaleFetchers extends Command

  private type CombinedCommand = Command | ManagementDataAggregator.Reply

  def apply(crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
            hostQueueRouter: ActorRef[HostQueue.Command],
            pageGateway: ActorRef[PageGateway.Command],
            urlNormalizer: ActorRef[UrlNormalizer.Command]): Behavior[Command] =
    Behaviors.setup[CombinedCommand](context => {
      Behaviors.withTimers(timers => {
        val config = context.system.settings.config
        val initialDelay = config.getDuration("abwcf.fetcher-manager.initial-delay").toScala
        val managementDelay = config.getDuration("abwcf.fetcher-manager.management-delay").toScala

        val managementDataAggregator = context.spawn(
          Behaviors.supervise(ManagementDataAggregator(context.self))
            .onFailure(SupervisorStrategy.restart),
          "management-data-aggregator"
        )

        //Periodically adjust the number of Fetchers:
        timers.startTimerWithFixedDelay(ScaleFetchers, initialDelay, managementDelay)

        new FetcherManager(crawlDepthLimiter, hostQueueRouter, pageGateway, urlNormalizer, managementDataAggregator, context).fetcherManager()
      })
    }).narrow
}

private class FetcherManager private (crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
                                      hostQueueRouter: ActorRef[HostQueue.Command],
                                      pageGateway: ActorRef[PageGateway.Command],
                                      urlNormalizer: ActorRef[UrlNormalizer.Command],
                                      managementDataAggregator: ActorRef[ManagementDataAggregator.Command],
                                      context: ActorContext[FetcherManager.CombinedCommand]) {
  import FetcherManager.*

  private val config = context.system.settings.config
  private val totalBytesPerSec = config.getBytes("abwcf.fetching.total-bandwidth-budget")
  private val minBytesPerSecPerFetcher = config.getBytes("abwcf.fetching.min-bandwidth-budget-per-fetcher")

  private val maxFetchersByBandwidth = (totalBytesPerSec / minBytesPerSecPerFetcher).toInt //Integer division with remainder. Cast because the rest of this code needs an Int.
  private val fetchers = mutable.ListBuffer.empty[ActorRef[Fetcher.Command]] //Mutable state!

  private def fetcherManager(): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case ScaleFetchers =>
      managementDataAggregator ! ManagementDataAggregator.GetManagementData
      Behaviors.same

    case ManagementDataAggregator.ManagementData(numHostQueues, numFetcherManagers) =>
      //Distribute Fetchers evenly across all FetcherManagers in the cluster if the number of HostQueues is low:
      val maxLocalFetchers = (numHostQueues / numFetcherManagers) + 1 //Add one to compensate for any remainders from the division.

      //Scale the number of Fetchers:
      val target = numHostQueues //Ensure that target ≤ numHostQueues.
        .min(maxLocalFetchers) //Ensure that target ≤ maxLocalFetchers.
        .min(maxFetchersByBandwidth) //Ensure that target ≤ maxFetchersByBandwidth.

      scaleFetchers(target)

      //Tell each Fetcher how much bandwidth it can use:
      val bandwidthUsers = fetchers.length.max(1) //Ensure that bandwidthUsers ≠ 0 (to avoid dividing by zero).
      val bytesPerSecPerFetcher = (totalBytesPerSec / bandwidthUsers).toInt //Integer division with remainder. Cast because the Fetcher needs an Int.
      fetchers.foreach(_ ! Fetcher.SetMaxBandwidth(bytesPerSecPerFetcher))

      context.log.info("Current number of Fetchers: {} (with up to {} B/s bandwidth each)", fetchers.length, bytesPerSecPerFetcher)
      Behaviors.same
  })

  /**
   * Spawns or stops [[Fetcher]] actors to reach a target number of [[Fetcher]] actors.
   */
  private def scaleFetchers(target: Int): Unit = {
    //Spawn more Fetchers if needed:
    while fetchers.length < target do {
      val fetcher = context.spawnAnonymous(
        Behaviors.supervise(Fetcher(crawlDepthLimiter, hostQueueRouter, pageGateway, urlNormalizer))
          .onFailure(SupervisorStrategy.restart)
      )

      fetchers.prepend(fetcher)
    }

    //Stop excess Fetchers if needed:
    while fetchers.length > target do {
      fetchers.head ! Fetcher.Stop
      fetchers.remove(0)
    }
  }
}
