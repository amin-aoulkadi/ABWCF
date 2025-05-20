package abwcf.actors

import abwcf.api.CrawlerSettings
import abwcf.data.PageCandidate
import abwcf.metrics.UrlDeduplicatorMetrics
import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

/**
 * Only sends URLs downstream that have not been seen recently.
 *
 * This actor can not guarantee complete deduplication - the URLs have not even been normalized yet. Complete deduplication is facilitated downstream by the [[PageManager]] actors and the cluster sharding mechanism.
 *
 * There should be one [[UrlDeduplicator]] actor per node.
 *
 * This actor is stateful.
 */
object UrlDeduplicator {
  sealed trait Command
  case class Deduplicate(candidate: PageCandidate) extends Command

  private case object DummyValue

  def apply(urlNormalizer: ActorRef[UrlNormalizer.Command], settings: CrawlerSettings): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val maxCacheSize = config.getLong("abwcf.actors.deduplicator.max-cache-size")

    val recentUrls = Caffeine.newBuilder() //Mutable state!
      .maximumSize(maxCacheSize)
      .recordStats() //Needed for metrics.
      .build[String, DummyValue.type]()

    val metrics = UrlDeduplicatorMetrics(settings, context, recentUrls)

    Behaviors.receiveMessage({
      case Deduplicate(candidate) =>
        val cachedValue = recentUrls.getIfPresent(candidate.url)

        if (cachedValue == null) { //Cache miss
          //Add the URL to the cache and send it downstream:
          recentUrls.put(candidate.url, DummyValue)
          urlNormalizer ! UrlNormalizer.Normalize(candidate)
          metrics.addFilterPassed(1)
        } else { //Cache hit - the URL is a duplicate.
          metrics.addFilterRejected(1)
        }

        Behaviors.same
    })
  })
}
