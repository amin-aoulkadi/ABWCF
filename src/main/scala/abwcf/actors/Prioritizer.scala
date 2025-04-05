package abwcf.actors

import abwcf.data.PageCandidate
import abwcf.util.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

/**
 * Assigns crawl priorities to [[PageCandidate]]s.
 *
 * There should be one [[Prioritizer]] actor per node.
 *
 * This actor is stateless.
 */
object Prioritizer {
  sealed trait Command
  case class Prioritize(candidate: PageCandidate) extends Command

  def apply(settings: CrawlerSettings, pageShardRegion: ActorRef[ShardingEnvelope[PageManager.Command]]): Behavior[Command] = {
    val prioritizationFunction = settings.prioritizationFunction
    
    Behaviors.receiveMessage({
      case Prioritize(candidate) =>
        val priority = prioritizationFunction(candidate)
        pageShardRegion ! ShardingEnvelope(candidate.url, PageManager.SetPriority(priority))
        Behaviors.same
    })
  }
}
