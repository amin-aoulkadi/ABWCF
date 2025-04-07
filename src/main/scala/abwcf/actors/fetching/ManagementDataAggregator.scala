package abwcf.actors.fetching

import abwcf.actors.HostQueue
import org.apache.pekko.actor.typed.pubsub.Topic
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.ShardRegion
import org.apache.pekko.cluster.sharding.typed.GetClusterShardingStats
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Aggregates relevant data from various sources for a [[FetcherManager]]. Uses distributed publish-subscribe to communicate with other [[ManagementDataAggregator]] actors in the cluster.
 *
 * There should be one [[ManagementDataAggregator]] actor per [[FetcherManager]] actor.
 *
 * This actor is stateful.
 */
object ManagementDataAggregator {
  sealed trait Command
  case object GetManagementData extends Command
  private case class Heartbeat(fetcherManager: ActorRef[FetcherManager.Command]) extends Command
  private case class RemoveFetcherManager(fetcherManager: ActorRef[FetcherManager.Command]) extends Command

  sealed trait Reply
  case class ManagementData(numHostQueues: Int, numFetcherManagers: Int) extends Reply

  private type CombinedCommand = Command | ShardRegion.ClusterShardingStats

  def apply(fetcherManager: ActorRef[FetcherManager.Command | Reply]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    //Subscribe to the distributed publish-subscribe topic for Heartbeats:
    val topic = context.spawn(Topic[Heartbeat]("FetcherManagerHeartbeats"), "fetcher-manager-heartbeats-topic")
    topic ! Topic.Subscribe(context.self)

    val clusterFetcherManagers = mutable.HashSet[ActorRef[FetcherManager.Command]](fetcherManager) //Mutable state!

    Behaviors.receiveMessage({
      case GetManagementData =>
        ClusterSharding(context.system).shardState ! GetClusterShardingStats(HostQueue.TypeKey, 5 seconds, context.self) //Request sharding statistics for HostQueues.
        topic ! Topic.Publish(Heartbeat(fetcherManager)) //Tell other subscribers that the parent FetcherManager is (still) active.
        Behaviors.same

      case ShardRegion.ClusterShardingStats(regions) =>
        //Calculate the number of HostQueues in the cluster:
        val numHostQueues = regions.values
          .map(shardRegionStats => shardRegionStats.stats)
          .flatMap(shardStats => shardStats.values)
          .sum

        fetcherManager ! ManagementData(numHostQueues, clusterFetcherManagers.size)
        Behaviors.same

      case Heartbeat(fetcherManager) =>
        clusterFetcherManagers.add(fetcherManager)
        context.watchWith(fetcherManager, RemoveFetcherManager(fetcherManager)) //Watch the FetcherManager in order to be notified when it terminates.
        Behaviors.same

      case RemoveFetcherManager(fetcherManager) =>
        clusterFetcherManagers.remove(fetcherManager)
        Behaviors.same
    })
  }).narrow
}
