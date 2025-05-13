package abwcf.metrics

import abwcf.actors.StrictRobotsFilter
import abwcf.util.CrawlerSettings
import com.github.benmanes.caffeine.cache.Cache
import org.apache.pekko.actor.typed.scaladsl.ActorContext

object StrictRobotsFilterMetrics {
  private val Prefix = "abwcf.strict_robots_filter"

  def apply(settings: CrawlerSettings, context: ActorContext[?], cache: Cache[?, ?]): StrictRobotsFilterMetrics = {
    new StrictRobotsFilterMetrics(settings, context, cache)
  }
}

class StrictRobotsFilterMetrics private (settings: CrawlerSettings, context: ActorContext[?], cache: Cache[?, ?]) extends ActorMetrics(context) {
  import StrictRobotsFilterMetrics.*

  private val meter = settings.openTelemetry.getMeter(StrictRobotsFilter.getClass.getName)
  private val filterMetrics = FilterMetrics(meter, Prefix)
  
  CacheMetrics.build(cache, meter, Prefix, actorAttributes)
  
  private val ignoredPagesCounter = meter.counterBuilder(s"$Prefix.ignored_pages")
    .setDescription("The number of pages that were ignored due to unavailable host information.")
    .build()
  
  def addFilterPassed(value: Long): Unit =
    filterMetrics.addFilterPassed(value, actorAttributes)

  def addFilterRejected(value: Long): Unit =
    filterMetrics.addFilterRejected(value, actorAttributes)
    
  def addIgnoredPages(value: Long): Unit =
    ignoredPagesCounter.add(value, actorAttributes)
}
