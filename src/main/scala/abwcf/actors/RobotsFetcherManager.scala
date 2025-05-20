package abwcf.actors

import abwcf.api.CrawlerSettings
import abwcf.metrics.RobotsFetcherManagerMetrics
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}

import scala.collection.mutable

/**
 * Manages [[RobotsFetcher]] actors.
 *
 * There should be one [[RobotsFetcherManager]] actor per node.
 *
 * This actor is stateful.
 */
object RobotsFetcherManager {
  sealed trait Command
  case class Fetch(schemeAndAuthority: String) extends Command
  private case object RobotsFetcherDone extends Command

  def apply(settings: CrawlerSettings): Behavior[Command] = Behaviors.setup(context => {
    new RobotsFetcherManager(settings, context).robotsFetcherManager()
  })
}

private class RobotsFetcherManager private (settings: CrawlerSettings, context: ActorContext[RobotsFetcherManager.Command]) {
  import RobotsFetcherManager.*

  private val config = context.system.settings.config
  private val maxActiveFetchers = config.getInt("abwcf.robots.fetching.max-concurrent-files")
  private val metrics = RobotsFetcherManagerMetrics(settings, context)

  private val queue = mutable.Queue.empty[String] //Mutable state!
  private var activeFetchers = 0 //Mutable state!

  metrics.buildQueueLengthGauge(queue)

  private def robotsFetcherManager(): Behavior[Command] = Behaviors.receiveMessage({
    case Fetch(schemeAndAuthority) if activeFetchers < maxActiveFetchers =>
      spawnRobotsFetcher(schemeAndAuthority)
      Behaviors.same

    case Fetch(schemeAndAuthority) =>
      queue.enqueue(schemeAndAuthority)
      Behaviors.same

    case RobotsFetcherDone if queue.nonEmpty =>
      activeFetchers -= 1
      val schemeAndAuthority = queue.dequeue()
      spawnRobotsFetcher(schemeAndAuthority)
      Behaviors.same

    case RobotsFetcherDone =>
      activeFetchers -= 1
      metrics.setFetchers(activeFetchers)
      Behaviors.same
  })

  private def spawnRobotsFetcher(schemeAndAuthority: String): Unit = {
    val robotsFetcher = context.spawnAnonymous(RobotsFetcher(schemeAndAuthority))
    context.watchWith(robotsFetcher, RobotsFetcherDone)
    activeFetchers += 1
    metrics.setFetchers(activeFetchers)
  }
}
