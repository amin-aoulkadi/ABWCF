package abwcf.actors

import abwcf.PageCandidate
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

import scala.util.Random

/**
 * Assigns crawl priorities to pages.
 *
 * There should be one [[Prioritizer]] actor per node.
 *
 * This actor is stateless.
 */
object Prioritizer {
  sealed trait Command
  case class Prioritize(candidate: PageCandidate) extends Command

//  private var sequenceNumber = Int.MaxValue

  def apply(pageShardRegion: ActorRef[ShardingEnvelope[Page.Command]]): Behavior[Command] = Behaviors.receiveMessage({
    case Prioritize(page) =>
      val priority = Random.nextInt() //TODO: Provide an API to inject user-defined code.
      pageShardRegion ! ShardingEnvelope(page.url, Page.SetPriority(priority))
//      pageShardRegion ! ShardingEnvelope(page.url, Page.SetPriority(sequenceNumber))
//      sequenceNumber = sequenceNumber - 1
      Behaviors.same
  })
}
