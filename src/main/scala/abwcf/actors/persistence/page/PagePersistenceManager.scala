package abwcf.actors.persistence.page

import abwcf.actors.persistence.page.PagePersistence.{InsertCommand, ReadCommand, UpdateCommand}
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
      case command: InsertCommand =>
        pageInserter ! command
        Behaviors.same

      case command: ReadCommand =>
        pageReader ! command
        Behaviors.same
        
      case command: UpdateCommand =>
        pageUpdater ! command
        Behaviors.same
    })
  })
}
