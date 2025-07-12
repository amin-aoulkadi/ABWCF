package abwcf.api

import abwcf.actors.{FetchResultConsumer, Prioritizer}
import abwcf.data.{FetchResponse, Page}
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.http.scaladsl.model.StatusCode

/**
 * API for providing user-defined code to the ABWCF.
 *
 * '''All code provided via this API is executed by actors and therefore must not block.'''
 */
trait UserCode {
  /**
   * Executed when the crawler receives a successful HTTP response while fetching a page.
   *
   * Limited API: This method is only suitable for very simple use cases. Use [[createFetchResultConsumer]] for more advanced use cases.
   *
   * This method is used by the [[FetchResultConsumer]] actor.
   *
   * The default implementation does nothing.
   */
  def onFetchSuccess(page: Page, response: FetchResponse, context: ActorContext[?]): Unit = ()

  /**
   * Executed when the crawler receives an HTTP redirection response while fetching a page.
   *
   * Limited API: This method is only suitable for very simple use cases. Use [[createFetchResultConsumer]] for more advanced use cases.
   *
   * This method is used by the [[FetchResultConsumer]] actor.
   *
   * The default implementation does nothing.
   */
  def onFetchRedirect(page: Page, statusCode: StatusCode, redirectTo: Option[String], context: ActorContext[?]): Unit = ()

  /**
   * Executed when the crawler receives an HTTP error response while fetching a page.
   *
   * Limited API: This method is only suitable for very simple use cases. Use [[createFetchResultConsumer]] for more advanced use cases.
   *
   * This method is used by the [[FetchResultConsumer]] actor.
   *
   * The default implementation does nothing.
   */
  def onFetchError(page: Page, statusCode: StatusCode, context: ActorContext[?]): Unit = ()

  /**
   * Executed when the crawler aborts fetching a page because the response body exceeds the maximum accepted content length.
   *
   * Limited API: This method is only suitable for very simple use cases. Use [[createFetchResultConsumer]] for more advanced use cases.
   *
   * This method is used by the [[FetchResultConsumer]] actor.
   *
   * The default implementation does nothing.
   */
  def onLengthLimitExceeded(page: Page, response: FetchResponse, context: ActorContext[?]): Unit = ()

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
   *             case Prioritizer.Prioritize(candidate) =>
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
   * The default implementation creates a new [[FetchResultConsumer]].
   *
   * @note Implementations must always notify the relevant [[abwcf.actors.PageManager]] with a [[abwcf.actors.PageManager.SetStatus]] message after processing a [[FetchResult.Command]]:
   *       {{{
   *         override def createFetchResultConsumer(settings: CrawlerSettings): Behavior[FetchResult.Command] = Behaviors.setup(context => {
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
  def createFetchResultConsumer(settings: CrawlerSettings): Behavior[FetchResult.Command] =
    FetchResultConsumer(settings)
}
