package abwcf.actors.persistence.page

import abwcf.actors.persistence.page.PagePersistence.{FindByStatus, Insert, Recover, UpdateStatus}
import abwcf.persistence.PageRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

/**
 * Manages communication with the [[abwcf.data.Page]] database.
 *
 * There should be one [[PagePersistenceManager]] actor per node.
 *
 * This actor is stateless.
 */
object PagePersistenceManager {
  def apply(pageRepository: PageRepository): Behavior[PagePersistence.Command] = Behaviors.setup(context => {
    val pageInserter = context.spawnAnonymous(PageInserter(pageRepository)) //TODO: Supervise.
    val pageReader = context.spawnAnonymous(PageReader(pageRepository))
    val pageUpdater = context.spawnAnonymous(PageUpdater(pageRepository))

    Behaviors.receiveMessage({
      case Insert(page) =>
        pageInserter ! Insert(page)
        Behaviors.same

      case UpdateStatus(url, status) =>
        pageUpdater ! UpdateStatus(url, status)
        Behaviors.same

      case Recover(url) =>
        pageReader ! Recover(url)
        Behaviors.same

      case FindByStatus(status, limit, replyTo) =>
        pageReader ! FindByStatus(status, limit, replyTo)
        Behaviors.same
    })
  })
}
