package abwcf.actors

import abwcf.actors.persistence.page.PagePersistence
import abwcf.data.PageStatus
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.ClusterSharding

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

  private type CombinedCommand = Command | PagePersistence.ResultSeq

  def apply(pagePersistenceManager: ActorRef[PagePersistence.Command]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    Behaviors.withTimers(timers => {
      val sharding = ClusterSharding(context.system)
      val config = context.system.settings.config
      val initialDelay = config.getDuration("abwcf.actors.page-restorer.initial-delay").toScala
      val restoreDelay = config.getDuration("abwcf.actors.page-restorer.restore-delay").toScala

      //Periodically attempt to restore pages:
      timers.startTimerWithFixedDelay(RestorePages, initialDelay, restoreDelay)

      Behaviors.receiveMessage({
        case RestorePages =>
          pagePersistenceManager ! PagePersistence.FindByStatus(PageStatus.Discovered, 100, context.self)
          Behaviors.same

        case PagePersistence.ResultSeq(pages) =>
          context.log.info("Restoring {} discovered pages (some may already be active)", pages.size)
          pages.foreach(page => {
            val pageManager = sharding.entityRefFor(PageManager.TypeKey, page.url)
            pageManager ! PageManager.RecoveryResult(Some(page))
          })
          Behaviors.same
      })
    })
  }).narrow
}
