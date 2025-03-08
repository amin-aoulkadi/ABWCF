package abwcf.actors

import org.apache.pekko.actor.typed.{Behavior, SupervisorStrategy}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.net.URISyntaxException

/**
 * This actor is the user guardian actor for the ABWCF.
 * 
 * There should be exactly one [[Crawler]] actor per node.
 */
object Crawler {
  sealed trait Command
  case class SeedUrls(urls: Seq[String]) extends Command
  
  def apply(): Behavior[Command] = Behaviors.setup(context => {
    val pageManager = context.spawn(
      Behaviors.supervise(PageManager())
        .onFailure(SupervisorStrategy.resume), //The PageManager is stateless, so resuming it is safe.
      "page-manager"
    )

    val urlFilter = context.spawn(
      Behaviors.supervise(UrlFilter(pageManager))
        .onFailure(SupervisorStrategy.resume), //The UrlFilter is stateless, so resuming it is safe.
      "url-filter"
    )
    
    val urlNormalizer = context.spawn(
      Behaviors.supervise(UrlNormalizer(urlFilter))
        .onFailure[URISyntaxException](SupervisorStrategy.resume), //The UrlNormalizer is stateless, so resuming it is safe.
      "url-normalizer"
    )
    
    Behaviors.receiveMessage({
      case SeedUrls(urls) =>
        urls.foreach(url => urlNormalizer ! UrlNormalizer.Normalize(url))
        Behaviors.same
    })
  })
}
