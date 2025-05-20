package abwcf.metrics

import abwcf.actors.UrlDeduplicator
import abwcf.api.CrawlerSettings
import com.github.benmanes.caffeine.cache.Cache
import org.apache.pekko.actor.typed.scaladsl.ActorContext

object UrlDeduplicatorMetrics {
  private val Prefix = "abwcf.url_deduplicator"

  def apply(settings: CrawlerSettings, context: ActorContext[?], cache: Cache[?, ?]): UrlDeduplicatorMetrics = {
    new UrlDeduplicatorMetrics(settings, context, cache)
  }
}

class UrlDeduplicatorMetrics private(settings: CrawlerSettings, context: ActorContext[?], cache: Cache[?, ?]) extends ActorMetrics(context) {
  import UrlDeduplicatorMetrics.*

  private val meter = settings.openTelemetry.getMeter(UrlDeduplicator.getClass.getName)
  private val filterMetrics = FilterMetrics(meter, Prefix)

  CacheMetrics.build(cache, meter, Prefix, actorAttributes)

  def addFilterPassed(value: Long): Unit =
    filterMetrics.addFilterPassed(value, actorAttributes)

  def addFilterRejected(value: Long): Unit =
    filterMetrics.addFilterRejected(value, actorAttributes)
}
