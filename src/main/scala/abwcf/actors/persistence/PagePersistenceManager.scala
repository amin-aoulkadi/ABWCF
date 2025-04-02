package abwcf.actors.persistence

import abwcf.actors.persistence.PagePersistence.{FindByStatus, Insert, Recover, UpdateStatus}
import abwcf.actors.{PageManager, PageGateway}
import abwcf.persistence.SlickPageRepository
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.slick.scaladsl.SlickSession

/**
 * Manages communication with the [[abwcf.PageEntity]] database.
 * 
 * There should be one [[PagePersistenceManager]] actor per node.
 * 
 * This actor uses a [[SlickSession]] internally, which must be closed explicitly to avoid leaking database resources.
 */
object PagePersistenceManager {
  def apply(pageShardRegion: ActorRef[ShardingEnvelope[PageManager.Command]]): Behavior[PagePersistence.Command] = Behaviors.setup(context => {
    val session = SlickSession.forConfig("postgres-slick") //TODO: Add to config.
    val materializer = Materializer(context)
    val pageRepository = new SlickPageRepository()(using session, materializer)

    val pageInserter = context.spawnAnonymous(PageInserter(pageRepository, pageShardRegion)) //TODO: Supervise.
    val pageReader = context.spawnAnonymous(PageReader(pageRepository, pageShardRegion))
    val pageUpdater = context.spawnAnonymous(PageUpdater(pageRepository, pageShardRegion))

    context.system.classicSystem.registerOnTermination(() => session.close()) //TODO: Maybe manage the session elsewhere.

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
