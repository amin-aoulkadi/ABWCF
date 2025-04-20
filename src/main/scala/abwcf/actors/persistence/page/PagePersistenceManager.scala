package abwcf.actors.persistence.page

import abwcf.actors.PageManager
import abwcf.actors.persistence.page.PagePersistence.{FindByStatus, Insert, Recover, UpdateStatus}
import abwcf.persistence.SlickPageRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.stream.Materializer

/**
 * Manages communication with the [[abwcf.data.Page]] database.
 *
 * There should be one [[PagePersistenceManager]] actor per node.
 *
 * This actor is stateless.
 */
object PagePersistenceManager {
  def apply(pageShardRegion: ActorRef[ShardingEnvelope[PageManager.Command]]): Behavior[PagePersistence.Command] = Behaviors.setup(context => {
    val materializer = Materializer(context)
    val pageRepository = new SlickPageRepository()(using materializer)

    val pageInserter = context.spawnAnonymous(PageInserter(pageRepository, pageShardRegion)) //TODO: Supervise.
    val pageReader = context.spawnAnonymous(PageReader(pageRepository, pageShardRegion))
    val pageUpdater = context.spawnAnonymous(PageUpdater(pageRepository, pageShardRegion))

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
