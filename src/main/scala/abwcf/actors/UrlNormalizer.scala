package abwcf.actors

import abwcf.api.CrawlerSettings
import abwcf.data.PageCandidate
import abwcf.metrics.UrlNormalizerMetrics
import abwcf.services.UrlNormalizationService
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

/**
 * Normalizes URLs and removes user information, query and fragment URL components as configured.
 *
 * There should be one [[UrlNormalizer]] actor per node.
 *
 * This actor is stateless.
 */
object UrlNormalizer {
  sealed trait Command
  case class Normalize(candidate: PageCandidate) extends Command

  def apply(urlFilter: ActorRef[UrlFilter.Command], settings: CrawlerSettings): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val removeUserInfo = config.getBoolean("abwcf.actors.url-normalizer.remove-userinfo")
    val removeQuery = config.getBoolean("abwcf.actors.url-normalizer.remove-query")
    val removeFragment = config.getBoolean("abwcf.actors.url-normalizer.remove-fragment")
    val logExceptions = config.getBoolean("abwcf.actors.url-normalizer.log-exceptions")
    val urlNormalizationService = new UrlNormalizationService(removeUserInfo, removeQuery, removeFragment)
    val metrics = UrlNormalizerMetrics(settings, context)

    Behaviors.receiveMessage({
      case Normalize(candidate) =>
        try {
          val normalizedUrl = urlNormalizationService.normalize(candidate.url)
          urlFilter ! UrlFilter.Filter(candidate.copy(url = normalizedUrl.toString))
        } catch {
          case e: Exception =>
            if logExceptions then context.log.error("Exception while normalizing URL {}", candidate.url, e)
            metrics.addException(e)
        }

        metrics.addProcessedUrls(1)
        Behaviors.same
    })
  })
}
