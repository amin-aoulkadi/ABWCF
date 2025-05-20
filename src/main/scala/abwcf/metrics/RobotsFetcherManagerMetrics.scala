package abwcf.metrics

import abwcf.api.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.ActorContext

import scala.collection.mutable

object RobotsFetcherManagerMetrics {
  private val Prefix = "abwcf.robots_fetcher_manager"

  def apply(settings: CrawlerSettings, context: ActorContext[?]): RobotsFetcherManagerMetrics = {
    new RobotsFetcherManagerMetrics(settings, context)
  }
}

class RobotsFetcherManagerMetrics private (settings: CrawlerSettings, context: ActorContext[?]) extends ActorMetrics(context) {
  import RobotsFetcherManagerMetrics.*

  private val meter = settings.openTelemetry.getMeter(RobotsFetcherManagerMetrics.getClass.getName)

  private val fetcherGauge = meter.gaugeBuilder(s"$Prefix.robots_fetchers")
    .setDescription("The number of RobotsFetchers.")
    .ofLongs()
    .build()
  
  def buildQueueLengthGauge(queue: mutable.Queue[?]): Unit = {
    meter.gaugeBuilder(s"$Prefix.queue.length")
      .setDescription("The length of the fetch queue.")
      .ofLongs()
      .buildWithCallback(_.record(queue.length, actorAttributes))
  }

  def setFetchers(value: Long): Unit =
    fetcherGauge.set(value, actorAttributes)
}
