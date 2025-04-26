package abwcf.util

import abwcf.data.{FetchResponse, Page, PageCandidate}
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.http.scaladsl.model.StatusCode

/**
 * API for providing user-defined code to the ABWCF.
 *
 * '''All code provided via this API is executed by actors and therefore must not block.'''
 */
trait UserCode {
  /**
   * Assigns a crawl priority to a [[PageCandidate]].
   *
   * Pages with a high crawl priority are more likely to be crawled than pages with a low crawl priority.
   *
   * This method is used by the [[abwcf.actors.Prioritizer]] actor.
   *
   * The default implementation is [[PrioritizationFunctions.random]].
   */
  def prioritize(candidate: PageCandidate, context: ActorContext[?]): Long =
    PrioritizationFunctions.random

  /**
   * Executed when the crawler receives a successful HTTP response while fetching a page.
   *
   * This method is used by the [[abwcf.actors.UserCodeRunner]] actor.
   *
   * The default implementation does nothing.
   */
  def onFetchSuccess(page: Page, response: FetchResponse, context: ActorContext[?]): Unit = ()

  /**
   * Executed when the crawler receives an HTTP redirection response while fetching a page.
   *
   * This method is used by the [[abwcf.actors.UserCodeRunner]] actor.
   *
   * The default implementation does nothing.
   */
  def onFetchRedirect(page: Page, statusCode: StatusCode, redirectTo: Option[String], context: ActorContext[?]): Unit = ()

  /**
   * Executed when the crawler receives an HTTP error response while fetching a page.
   *
   * This method is used by the [[abwcf.actors.UserCodeRunner]] actor.
   *
   * The default implementation does nothing.
   */
  def onFetchError(page: Page, statusCode: StatusCode, context: ActorContext[?]): Unit = ()

  /**
   * Executed when the crawler aborts fetching a page because the response body exceeds the maximum accepted content length.
   *
   * This method is used by the [[abwcf.actors.UserCodeRunner]] actor.
   *
   * The default implementation does nothing.
   */
  def onLengthLimitExceeded(page: Page, response: FetchResponse, context: ActorContext[?]): Unit = ()
}
