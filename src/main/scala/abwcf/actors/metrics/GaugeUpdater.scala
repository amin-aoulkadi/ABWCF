package abwcf.actors.metrics

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongGauge
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.ShardRegion

/**
 * Supports the [[ClusterNodeMetricsCollector]] by updating a [[LongGauge]] with the number of cluster sharding entities from a [[ShardRegion.CurrentShardRegionState]] message.
 *
 * This actor exists because [[ShardRegion.CurrentShardRegionState]] messages do not contain an [[org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey]] or similar information that would allow the message to be associated with an entity type.<br>
 * Message adapters are unfit for this purpose because there can only be one message adapter per message class.
 *
 * This actor is stateless.
 */
object GaugeUpdater {
  def apply(gauge: LongGauge, attributes: Attributes): Behavior[ShardRegion.CurrentShardRegionState] = Behaviors.receiveMessage({
    case shardRegionState: ShardRegion.CurrentShardRegionState => //ShardRegion.CurrentShardRegionState.unapply only returns (shards), not (shards, failed).
      val numEntities = shardRegionState.shards.toSeq //Without toSeq, the next step would produce a Set[Int], which would consider shards of the same size as duplicates.
        .map(state => state.entityIds.size)
        .sum

      gauge.set(numEntities, attributes)
      Behaviors.same
  })
}
