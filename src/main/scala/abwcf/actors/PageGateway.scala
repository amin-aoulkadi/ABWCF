package abwcf.actors

import abwcf.actors.persistence.page.{PagePersistence, PagePersistenceManager}
import abwcf.data.{FetchResponse, Page, PageCandidate}
import abwcf.util.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.http.scaladsl.model.StatusCode

/**
 * Gateway between [[PageManager]] actors and non-sharded actors.
 *
 * There should be one [[PageGateway]] actor per node.
 *
 * This actor is stateless.
 */
object PageGateway {
  sealed trait Command
  case class Discover(candidate: PageCandidate) extends Command
  case class FetchSuccess(page: Page, response: FetchResponse) extends Command
  case class FetchRedirect(page: Page, statusCode: StatusCode, redirectTo: Option[String]) extends Command
  case class FetchError(page: Page, statusCode: StatusCode) extends Command

  type CombinedCommand = Command | PagePersistence.Command | Prioritizer.Command

  def apply(settings: CrawlerSettings): Behavior[CombinedCommand] = Behaviors.setup(context => {
    val pageShardRegion = PageManager.getShardRegion(context.system, context.self)

    val prioritizer = context.spawn(
      Behaviors.supervise(Prioritizer(settings, pageShardRegion))
        .onFailure(SupervisorStrategy.resume), //The Prioritizer is stateless, so resuming it is safe.
      "prioritizer"
    )

    val pagePersistenceManager = context.spawn(
      Behaviors.supervise(PagePersistenceManager(pageShardRegion))
        .onFailure(SupervisorStrategy.resume), //Restarting would be problematic because the PagePersistenceManager internally creates a SlickSession that has to be closed explicitly.
      "page-persistence-manager"
    )

    val pageRestorer = context.spawn(
      Behaviors.supervise(PageRestorer(pageShardRegion, pagePersistenceManager))
        .onFailure(SupervisorStrategy.resume), //The PageRestorer is stateless, so resuming it is safe.
      "page-restorer"
    )

    val userCodeRunner = context.spawn(
      Behaviors.supervise(UserCodeRunner(settings, pageShardRegion))
        .onFailure(SupervisorStrategy.resume), //The UserCodeRunner is stateless, so resuming it is safe.
      "user-code-runner"
    )

    Behaviors.receiveMessage({
      case Discover(candidate) => //TODO: Add database lookup (with a small cache).
        pageShardRegion ! ShardingEnvelope(candidate.url, PageManager.Discover(candidate.crawlDepth))
        Behaviors.same

      case FetchSuccess(page, response) =>
        userCodeRunner ! UserCodeRunner.ProcessSuccess(page, response)
        Behaviors.same

      case FetchRedirect(page, statusCode, redirectTo) =>
        userCodeRunner ! UserCodeRunner.ProcessRedirect(page, statusCode, redirectTo)
        Behaviors.same

      case FetchError(page, statusCode) => //TODO: Retry fetching later. Maybe let the user code decide whether to retry or not.
        userCodeRunner ! UserCodeRunner.ProcessError(page, statusCode)
        Behaviors.same

      case command: PagePersistence.Command =>
        pagePersistenceManager ! command
        Behaviors.same

      case command: Prioritizer.Command =>
        prioritizer ! command
        Behaviors.same
    })
  })
}
