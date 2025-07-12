package abwcf.actors

import abwcf.data.PageCandidate
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

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

  def apply(prioritize: PageCandidate => Long): Behavior[Command] = Behaviors.setup(context => {
    val sharding = ClusterSharding(context.system)

    Behaviors.receiveMessage({
      case Prioritize(candidate) =>
        val priority = prioritize(candidate)
        val pageManager = sharding.entityRefFor(PageManager.TypeKey, candidate.url)
        pageManager ! PageManager.SetPriority(priority)
        Behaviors.same
    })
  })
}
