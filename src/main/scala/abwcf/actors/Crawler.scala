package abwcf.actors

import org.apache.pekko.actor.typed.{Behavior, SupervisorStrategy}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.net.URISyntaxException

/**
 * This actor is the user guardian actor for the ABWCF.
 */
object Crawler {
  sealed trait Command
  case class SeedUrls(urls: Seq[String]) extends Command
  
  def apply(): Behavior[Command] = Behaviors.setup(context => {
    val printActor = context.spawnAnonymous(Behaviors.receiveMessage[Any](msg => {
      println(msg)
      Behaviors.same
    }))
    
    val urlNormalizer = context.spawn(
      Behaviors.supervise(UrlNormalizer(printActor)).onFailure[URISyntaxException](SupervisorStrategy.resume), //The UrlNormalizer is stateless, so resuming it is safe.
      "url-normalizer"
    )
    
    Behaviors.receiveMessage({
      case SeedUrls(urls) =>
        urls.foreach(url => urlNormalizer ! UrlNormalizer.Normalize(url))
        Behaviors.same
    })
  })
}
