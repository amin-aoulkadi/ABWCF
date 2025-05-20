package abwcf.actors.metrics

import abwcf.actors.{HostManager, HostQueue, PageManager}
import abwcf.api.CrawlerSettings
import abwcf.metrics.AttributeKeys
import io.opentelemetry.api.common.Attributes
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.GetShardRegionState
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.jdk.DurationConverters.*

/**
 * Collects certain metrics at the cluster node level.
 *
 * There should be one [[ClusterNodeMetricsCollector]] actor per node.
 *
 * This actor is stateful.
 */
object ClusterNodeMetricsCollector {
  sealed trait Command
  private case object CollectMetrics extends Command

  def apply(settings: CrawlerSettings): Behavior[Command] = Behaviors.setup(context => {
    Behaviors.withTimers(timers => {
      val sharding = ClusterSharding(context.system)
      val config = context.system.settings.config
      val collectionDelay = config.getDuration("abwcf.actors.cluster-node-metrics-collector.collection-delay").toScala
      val meter = settings.openTelemetry.getMeter(ClusterNodeMetricsCollector.getClass.getName)
      val attributes = Attributes.of(AttributeKeys.ActorSystemAddress, context.system.address.toString)

      meter.gaugeBuilder("abwcf.actor_system.uptime")
        .setUnit("s")
        .setDescription("The uptime of the actor system.")
        .ofLongs()
        .buildWithCallback(_.record(context.system.uptime, attributes))

      val hostManagerGauge = meter.gaugeBuilder("abwcf.host_managers")
        .setDescription("The number of HostManagers on this node.")
        .ofLongs()
        .build()

      val hostQueueGauge = meter.gaugeBuilder("abwcf.host_queues")
        .setDescription("The number of HostQueues on this node.")
        .ofLongs()
        .build()

      val pageManagerGauge = meter.gaugeBuilder("abwcf.page_managers")
        .setDescription("The number of PageManagers on this node.")
        .ofLongs()
        .build()

      val hostManagerGaugeUpdater = context.spawnAnonymous(GaugeUpdater(hostManagerGauge, attributes))
      val hostQueueGaugeUpdater = context.spawnAnonymous(GaugeUpdater(hostQueueGauge, attributes))
      val pageManagerGaugeUpdater = context.spawnAnonymous(GaugeUpdater(pageManagerGauge, attributes))

      //Periodically collect metrics:
      timers.startTimerWithFixedDelay(CollectMetrics, collectionDelay)

      Behaviors.receiveMessage({
        case CollectMetrics =>
          //Request the states of the local shard regions:
          sharding.shardState ! GetShardRegionState(HostManager.TypeKey, hostManagerGaugeUpdater)
          sharding.shardState ! GetShardRegionState(HostQueue.TypeKey, hostQueueGaugeUpdater)
          sharding.shardState ! GetShardRegionState(PageManager.TypeKey, pageManagerGaugeUpdater)
          Behaviors.same
      })
    })
  })
}
