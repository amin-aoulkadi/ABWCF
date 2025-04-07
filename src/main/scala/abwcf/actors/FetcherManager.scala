package abwcf.actors

import org.apache.pekko.actor.typed.pubsub.Topic
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.cluster.sharding.ShardRegion
import org.apache.pekko.cluster.sharding.typed.GetClusterShardingStats
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.*
import scala.language.postfixOps

/**
 * Manages [[Fetcher]] actors. Uses distributed publish-subscribe to coordinate with other [[FetcherManager]] actors.
 *
 * There should be one [[FetcherManager]] actor per node.
 *
 * This actor is stateful.
 */
object FetcherManager {
  sealed trait Command
  private case object CheckHostQueues extends Command
  private case class FetcherManagerInfo(fetcherManager: ActorRef[Command], numFetchers: Int) extends Command
  private case class RemoveFetcherManagerInfo(fetcherManager: ActorRef[Command]) extends Command

  private type CombinedCommand = Command | ShardRegion.ClusterShardingStats

  def apply(crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
            hostQueueRouter: ActorRef[HostQueue.Command],
            pageGateway: ActorRef[PageGateway.Command],
            urlNormalizer: ActorRef[UrlNormalizer.Command]): Behavior[Command] =
    Behaviors.setup[CombinedCommand](context => {
      Behaviors.withTimers(timers => {
        val config = context.system.settings.config
        val initialDelay = config.getDuration("abwcf.fetcher-manager.initial-delay").toScala
        val managementDelay = config.getDuration("abwcf.fetcher-manager.management-delay").toScala

        //Subscribe to the distributed publish-subscribe topic for FetcherManagerInfos:
        val topic = context.spawn(Topic[FetcherManagerInfo]("FetcherManagerInfo"), "topic-fetcher-manager-info")
        topic ! Topic.Subscribe(context.self)

        //Periodically check the number of available HostQueues:
        timers.startTimerWithFixedDelay(CheckHostQueues, initialDelay, managementDelay)

        new FetcherManager(crawlDepthLimiter, hostQueueRouter, pageGateway, urlNormalizer, topic, context).fetcherManager()
      })
    }).narrow
}

private class FetcherManager private (crawlDepthLimiter: ActorRef[CrawlDepthLimiter.Command],
                                      hostQueueRouter: ActorRef[HostQueue.Command],
                                      pageGateway: ActorRef[PageGateway.Command],
                                      urlNormalizer: ActorRef[UrlNormalizer.Command],
                                      topic: ActorRef[Topic.Command[FetcherManager.FetcherManagerInfo]],
                                      context: ActorContext[FetcherManager.CombinedCommand]) {
  import FetcherManager.*

  private val config = context.system.settings.config
  private val totalBytesPerSec = config.getBytes("abwcf.fetcher-manager.total-budget")
  private val minBytesPerSecPerFetcher = config.getBytes("abwcf.fetcher-manager.min-budget-per-fetcher")

  private val maxFetchersByBandwidth = (totalBytesPerSec / minBytesPerSecPerFetcher).toInt //Integer division with remainder. Cast because the rest of this code needs an Int.
  private val fetchers = mutable.ListBuffer.empty[ActorRef[Fetcher.Command]]
  private val clusterFetcherManagerInfos = mutable.HashMap.empty[ActorRef[Command], FetcherManagerInfo]

  private def fetcherManager(): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case CheckHostQueues =>
      //Check the number of available HostQueues:
      ClusterSharding(context.system).shardState ! GetClusterShardingStats(HostQueue.TypeKey, 5 seconds, context.self)
      Behaviors.same

    case ShardRegion.ClusterShardingStats(regions) =>
      //Determine the number of HostQueues in the cluster:
      val numHostQueues = regions.values
        .map(shardRegionStats => shardRegionStats.stats)
        .flatMap(shardStats => shardStats.values)
        .sum

      //TODO:
//      val numFetchersInCluster = clusterFetcherManagerInfos.values
//        .map(_.numFetchers)
//        .sum

      //Distribute Fetchers evenly across all FetcherManagers in the cluster if the number of HostQueues is low:
      val maxLocalFetchers = (numHostQueues / math.max(1, clusterFetcherManagerInfos.size)) + 1 //Add one to compensate for any remainders from the division.
      context.log.info("numHostQueues: {}, maxLocalFetchers: {}, maxFetchersByBandwidth: {}", numHostQueues, maxLocalFetchers, maxFetchersByBandwidth)

      //Scale the number of Fetchers:
      val target = numHostQueues //Ensure that target ≤ numHostQueues.
        .min(maxLocalFetchers) //Ensure that target ≤ maxLocalFetchers.
        .min(maxFetchersByBandwidth) //Ensure that target ≤ maxFetchersByBandwidth.

      scaleFetchers(target)

      //Tell each Fetcher how much bandwidth it can use:
      val bytesPerSecPerFetcher = (totalBytesPerSec / target).toInt //Integer division with remainder. Cast because the Fetcher needs an Int.
      fetchers.foreach(_ ! Fetcher.SetMaxBandwidth(bytesPerSecPerFetcher))

      context.log.info("Current number of Fetchers: {} (with up to {} B/s bandwidth each)", fetchers.length, bytesPerSecPerFetcher)
      topic ! Topic.Publish(FetcherManagerInfo(context.self, target))
      Behaviors.same

    case info @ FetcherManagerInfo(fetcherManager, _) =>
      clusterFetcherManagerInfos.update(fetcherManager, info)
      context.watchWith(fetcherManager, RemoveFetcherManagerInfo(fetcherManager)) //Watch the other FetcherManager in order to be notified when it terminates.
      Behaviors.same

    case RemoveFetcherManagerInfo(fetcherManager) =>
      clusterFetcherManagerInfos.remove(fetcherManager)
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
