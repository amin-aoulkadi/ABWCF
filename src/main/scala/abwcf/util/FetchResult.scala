package abwcf.util

import abwcf.data.{FetchResponse, Page}
import org.apache.pekko.http.scaladsl.model.StatusCode

/**
 * The protocol of actors that process fetch results.
 */
object FetchResult {
  sealed trait Command
  case class Success(page: Page, response: FetchResponse) extends Command
  case class Redirect(page: Page, statusCode: StatusCode, redirectTo: Option[String]) extends Command
  case class Error(page: Page, statusCode: StatusCode) extends Command
  case class LengthLimitExceeded(page: Page, response: FetchResponse) extends Command
}
