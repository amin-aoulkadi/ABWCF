package abwcf.metrics

import abwcf.api.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.ActorContext

object FetcherManagerMetrics {
  private val Prefix = "abwcf.fetcher_manager"

  def apply(settings: CrawlerSettings, context: ActorContext[?]): FetcherManagerMetrics = {
    new FetcherManagerMetrics(settings, context)
  }
}

class FetcherManagerMetrics private (settings: CrawlerSettings, context: ActorContext[?]) extends ActorMetrics(context) {
  import FetcherManagerMetrics.*

  private val meter = settings.openTelemetry.getMeter(FetcherManagerMetrics.getClass.getName)

  private val fetcherGauge = meter.gaugeBuilder(s"$Prefix.fetcher.count")
    .setDescription("The number of Fetchers.")
    .ofLongs()
    .build()

  private val fetcherBandwidthGauge = meter.gaugeBuilder(s"$Prefix.fetcher.bandwidth_budget")
    .setUnit("B/s") //Bytes per second; UCUM apparently has no identifier for this.
    .setDescription("The bandwidth budget per Fetcher.")
    .ofLongs()
    .build()

  def setFetchers(value: Long): Unit =
    fetcherGauge.set(value, actorAttributes)

  def setFetcherBandwidth(value: Long): Unit =
    fetcherBandwidthGauge.set(value, actorAttributes)
}
