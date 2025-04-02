package abwcf.actors

import abwcf.PageCandidate
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

/**
 * Filters out URLs that should not be crawled based on URL length and regular expressions.
 *
 * There should be one [[UrlFilter]] actor per node.
 *
 * This actor is stateless.
 *
 * @see [[java.util.regex.Pattern]]
 */
object UrlFilter {
  sealed trait Command
  case class Filter(page: PageCandidate) extends Command

  def apply(pageManager: ActorRef[PageManager.Command]): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val maxUrlLength = config.getInt("abwcf.url-filter.max-url-length")

    val mustMatch = config.getStringList("abwcf.url-filter.must-match")
      .asScala
      .map(Regex(_))

    val mustNotMatch = config.getStringList("abwcf.url-filter.must-not-match")
      .asScala
      .map(Regex(_))

    Behaviors.receiveMessage({
      case Filter(page) =>
        if (page.url.length <= maxUrlLength) {
          val existsRequiredMatch = mustMatch.exists(_.matches(page.url))
          val existsForbiddenMatch = mustNotMatch.exists(_.matches(page.url))

          if (existsRequiredMatch && !existsForbiddenMatch) {
            pageManager ! PageManager.Discover(page)
          }
        }

        Behaviors.same
    })
  })
}
