package abwcf.actors

import abwcf.PageStatus
import abwcf.actors.persistence.{PagePersistenceManager, PageReader}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope

import scala.jdk.DurationConverters.*

/**
 * Periodically restores discovered pages from the database to give them a chance to be processed.
 *
 * There should be one [[PageRestorer]] actor per node.
 *
 * This actor is stateless.
 */
object PageRestorer {
  sealed trait Command
  private case object RestorePages extends Command

  private type CombinedCommand = Command | PageReader.ResultSeq

  def apply(pagePersistenceManager: ActorRef[PagePersistenceManager.Command]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    Behaviors.withTimers(timers => {
      val config = context.system.settings.config
      val initialDelay = config.getDuration("abwcf.page-restorer.initial-delay").toScala
      val restoreDelay = config.getDuration("abwcf.page-restorer.restore-delay").toScala
      val pageShardRegion = Page.getShardRegion(context.system, pagePersistenceManager)

      //Periodically attempt to restore pages:
      timers.startTimerWithFixedDelay(RestorePages, initialDelay, restoreDelay)

      Behaviors.receiveMessage({
        case RestorePages =>
          pagePersistenceManager ! PagePersistenceManager.FindByStatus(PageStatus.Discovered, 100, context.self)
          Behaviors.same

        case PageReader.ResultSeq(pages) =>
          context.log.info("Restoring {} discovered pages (some may already be active)", pages.size)
          pages.foreach(page => pageShardRegion ! ShardingEnvelope(page.url, Page.RecoveryResult(Some(page))))
          Behaviors.same
      })
    })
  }).narrow
}
