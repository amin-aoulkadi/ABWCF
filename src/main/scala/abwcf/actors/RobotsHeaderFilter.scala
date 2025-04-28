package abwcf.actors

import abwcf.data.{FetchResponse, Page}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

/**
 * Filters out responses that should not be parsed based on `X-Robots-Tag` response headers.
 *
 * There should be one [[RobotsHeaderFilter]] actor per node.
 *
 * This actor is stateless.
 *
 * @note There is no official standard or specification for `X-Robots-Tag` headers, so each vendor supports a different set of rules.
 * @see
 *      - [[https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/X-Robots-Tag MDN: X-Robots-Tag]]
 *      - [[https://developers.google.com/search/docs/crawling-indexing/robots-meta-tag Google: Robots Meta Tags Specifications]]
 */
object RobotsHeaderFilter {
  sealed trait Command
  case class Filter(page: Page, response: FetchResponse) extends Command

  def apply(htmlParser: ActorRef[HtmlParser.Command]): Behavior[Command] = Behaviors.setup(context => {
    Behaviors.receiveMessage({
      case Filter(page, response) =>
        //Check if there are any X-Robots-Tag headers that indicate that robots should not follow the links in the response:
        val nofollow = response.headers
          .filter(_.is("x-robots-tag")) //Expects the header name in lowercase.
          .flatMap(_.value.split(',')) //["noindex", " nofollow"]
          .map(_.trim) //["noindex", "nofollow"]
          .exists(_.equalsIgnoreCase("nofollow"))

        if (!nofollow) {
          htmlParser ! HtmlParser.Parse(page, response.body)
        }

        Behaviors.same
    })
  })
}
