package abwcf.actors

import abwcf.data.{FetchResponse, Page}
import abwcf.services.robots_tag.RobotsTagParsingService
import abwcf.util.RobotsUtils
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import scala.jdk.CollectionConverters.*

/**
 * Filters out responses that should not be parsed based on `X-Robots-Tag` response headers.
 *
 * There should be one [[RobotsHeaderFilter]] actor per node.
 *
 * This actor is stateless.
 */
object RobotsHeaderFilter {
  sealed trait Command
  case class Filter(page: Page, response: FetchResponse) extends Command

  def apply(htmlParser: ActorRef[HtmlParser.Command]): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val userAgents = config.getStringList("abwcf.robots.user-agents").asScala.toSet
    val robotsTagParser = RobotsTagParsingService(userAgents)

    Behaviors.receiveMessage({
      case Filter(page, response) =>
        //Check if there are any X-Robots-Tag headers that indicate that robots should not follow the links in the response:
        robotsTagParser.reset()

        response.headers
          .filter(_.is("x-robots-tag")) //Expects the header name in lowercase.
          .foreach(header => robotsTagParser.parse(header.value))

        val canFollowLinks = RobotsUtils.canFollowLinks(robotsTagParser.collectedDirectives)

        if (canFollowLinks) {
          htmlParser ! HtmlParser.Parse(page, response.body)
        }

        Behaviors.same
    })
  })
}
