package abwcf.data

import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.util.ByteString

/**
 * A complete HTTP response.
 *
 * @note One important purpose of this class is to avoid exposing the [[ResponseEntity]] after fetching. This is necessary because the [[ResponseEntity]] must not be consumed again after fetching (see [[ResponseEntity.discardBytes]]).
 */
case class FetchResponse(
                          body: ByteString,
                          contentType: ContentType,
                          headers: Seq[HttpHeader],
                          protocol: HttpProtocol,
                          status: StatusCode
                        ) {
  def this(httpResponse: HttpResponse, body: ByteString) = {
    this(body, httpResponse.entity.contentType, httpResponse.headers, httpResponse.protocol, httpResponse.status)
  }
}
