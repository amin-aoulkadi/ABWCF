package abwcf.metrics

import abwcf.actors.LenientRobotsFilter
import abwcf.util.CrawlerSettings
import com.github.benmanes.caffeine.cache.Cache
import org.apache.pekko.actor.typed.scaladsl.ActorContext

object LenientRobotsFilterMetrics {
  private val Prefix = "abwcf.lenient_robots_filter"

  def apply(settings: CrawlerSettings, context: ActorContext[?], cache: Cache[?, ?]): LenientRobotsFilterMetrics = {
    new LenientRobotsFilterMetrics(settings, context, cache)
  }
}

class LenientRobotsFilterMetrics private (settings: CrawlerSettings, context: ActorContext[?], cache: Cache[?, ?]) extends ActorMetrics(context) {
  import LenientRobotsFilterMetrics.*

  private val meter = settings.openTelemetry.getMeter(LenientRobotsFilter.getClass.getName)
  private val filterMetrics = FilterMetrics(meter, Prefix)
  
  CacheMetrics.build(cache, meter, Prefix, actorAttributes)

  def addFilterPassed(value: Long): Unit =
    filterMetrics.addFilterPassed(value, actorAttributes)

  def addFilterRejected(value: Long): Unit =
    filterMetrics.addFilterRejected(value, actorAttributes)
}
