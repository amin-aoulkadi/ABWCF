package abwcf.api

import abwcf.actors.{FetchResultConsumer, Prioritizer}
import org.apache.pekko.actor.typed.Behavior

/**
 * API for providing user-defined code to the ABWCF.
 *
 * '''All code provided via this API is executed by actors and therefore must not block.'''
 */
trait UserCode {
  /**
   * Creates an actor that assigns crawl priorities to [[abwcf.data.PageCandidate]]s.
   *
   * Pages with a high crawl priority are more likely to be crawled than pages with a low crawl priority.
   *
   * The default implementation creates a new [[Prioritizer]] that uses [[PrioritizationFunctions.random]].
   *
   * @note Implementations must always send a [[abwcf.actors.PageManager.SetPriority]] message with the crawl priority to the relevant [[abwcf.actors.PageManager]]:
   *       {{{
   *         override def createPrioritizer(settings: CrawlerSettings): Behavior[Prioritizer.Command] = Behaviors.setup(context => {
   *           val sharding = ClusterSharding(context.system)
   *
   *           Behaviors.receiveMessage({
   *             case Prioritize(candidate) =>
   *               val priority = 123
   *               val pageManager = sharding.entityRefFor(PageManager.TypeKey, candidate.url)
   *               pageManager ! PageManager.SetPriority(priority)
   *               Behaviors.same
   *           })
   *         })
   *       }}}
   */
  def createPrioritizer(settings: CrawlerSettings): Behavior[Prioritizer.Command] =
    Prioritizer(_ => PrioritizationFunctions.random)

  /**
   * Creates a new fetch result consumer.
   *
   * The default implementation creates a new [[FetchResultConsumer]] that does nothing (except notify the [[abwcf.actors.PageManager]]).
   *
   * @note Implementations must always notify the relevant [[abwcf.actors.PageManager]] with a [[abwcf.actors.PageManager.SetStatus]] message after processing a [[FetchResultConsumer.Command]]:
   *       {{{
   *         override def createFetchResultConsumer(settings: CrawlerSettings): Behavior[FetchResultConsumer.Command] = Behaviors.setup(context => {
   *           val sharding = ClusterSharding(context.system)
   *
   *           def notifyPageManager(page: Page): Unit = {
   *             val pageManager = sharding.entityRefFor(PageManager.TypeKey, page.url)
   *             pageManager ! PageManager.SetStatus(PageStatus.Processed)
   *           }
   *
   *           Behaviors.receiveMessage({
   *             case Success(page, response) =>
   *               //...
   *               notifyPageManager(page)
   *               Behaviors.same
   *
   *             case Redirect(page, statusCode, redirectTo) =>
   *               //...
   *               notifyPageManager(page)
   *               Behaviors.same
   *
   *             //...
   *           })
   *         })
   *       }}}
   */
  def createFetchResultConsumer(settings: CrawlerSettings): Behavior[FetchResultConsumer.Command] =
    FetchResultConsumer()
}
