package abwcf.actors

import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.cluster.sharding.ShardRegion
import org.apache.pekko.cluster.sharding.typed.GetClusterShardingStats
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.collection.immutable.List
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Manages [[Fetcher]] actors.
 *
 * There should be one [[FetcherManager]] actor per node.
 *
 * This actor is stateful.
 */
object FetcherManager {
  sealed trait Command
  private case object CheckHostQueues extends Command

  private type CombinedCommand = Command | ShardRegion.ClusterShardingStats

  def apply(crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
            hostQueueRouter: ActorRef[HostQueue.Command],
            urlNormalizer: ActorRef[UrlNormalizer.Command]): Behavior[Command] =
    Behaviors.setup[CombinedCommand](context => {
      Behaviors.withTimers(timers => {
        //Periodically check the number of available HostQueues:
        timers.startTimerWithFixedDelay(CheckHostQueues, 5 seconds, 10 seconds) //TODO: Add to config.

        new FetcherManager(crawlDepthLimiter, hostQueueRouter, urlNormalizer, context).fetcherManager(List.empty)
      })
    }).narrow
}

private class FetcherManager private (crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
                                      hostQueueRouter: ActorRef[HostQueue.Command],
                                      urlNormalizer: ActorRef[UrlNormalizer.Command],
                                      context: ActorContext[FetcherManager.CombinedCommand]) {
  import FetcherManager.*

  private def fetcherManager(fetchers: List[ActorRef[Fetcher.Command]]): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case CheckHostQueues =>
      //Check the number of available HostQueues:
      ClusterSharding(context.system).shardState ! GetClusterShardingStats(HostQueue.TypeKey, 5 seconds, context.self)
      Behaviors.same

    case ShardRegion.ClusterShardingStats(regions) =>
      //Scale the number of Fetchers based on the number of HostQueues and processors:
      val numHostQueues = regions.values
        .map(shardRegionStats => shardRegionStats.stats)
        .flatMap(shardStats => shardStats.values)
        .sum

      val target = math.min(numHostQueues, Runtime.getRuntime.availableProcessors)
      var scaledFetchers = List.from(fetchers)

      //Spawn more Fetchers if needed:
      while scaledFetchers.length < target do {
        val fetcher = context.spawnAnonymous( //TODO: Dedicated thread pool for async fetching? → https://pekko.apache.org/docs/pekko/current/index-network.html
          Behaviors.supervise(Fetcher(crawlDepthLimiter, hostQueueRouter, urlNormalizer))
            .onFailure(SupervisorStrategy.restart.withLoggingEnabled(true))
        )
        
        scaledFetchers = scaledFetchers.prepended(fetcher)
      }

      //Stop excess Fetchers if needed:
      while scaledFetchers.length > target do {
        scaledFetchers.head ! Fetcher.Stop
        scaledFetchers = scaledFetchers.tail
      }

      context.log.info("Current number of Fetchers: {}", scaledFetchers.length)
      fetcherManager(scaledFetchers)
  })
}
