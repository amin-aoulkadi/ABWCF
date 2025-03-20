package abwcf.actors

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

/**
 * Uses configurable regular expressions to filter out URLs that should not be crawled.
 * 
 * There should be one [[UrlFilter]] actor per node.
 * 
 * This actor is stateless.
 * 
 * @see [[java.util.regex.Pattern]]
 */
object UrlFilter {
  sealed trait Command
  case class Filter(url: String, crawlDepth: Int) extends Command

  def apply(pageManager: ActorRef[PageManager.Command]): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    
    val mustMatch = config.getStringList("abwcf.url-filter.must-match")
      .asScala
      .map(Regex(_))
    
    val mustNotMatch = config.getStringList("abwcf.url-filter.must-not-match")
      .asScala
      .map(Regex(_))

    Behaviors.receiveMessage({
      case Filter(url, crawlDepth) =>
        val existsRequiredMatch = mustMatch.exists(_.matches(url))
        val existsForbiddenMatch = mustNotMatch.exists(_.matches(url))

        if (existsRequiredMatch && !existsForbiddenMatch) {
          pageManager ! PageManager.Discover(url, crawlDepth)
        }
        
        Behaviors.same
    })
  })
}
