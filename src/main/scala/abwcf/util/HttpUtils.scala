package abwcf.util

import org.apache.pekko.http.scaladsl.model.headers.Location
import org.apache.pekko.http.scaladsl.model.{HttpResponse, Uri}

object HttpUtils {
  /**
   * Returns the redirection URL from the `Location` response header.
   *
   * @param response the response from the server
   * @param url      the URL of the original request
   * @see
   *      - [[https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Location MDN: Location]]
   *      - [[https://datatracker.ietf.org/doc/html/rfc9110 RFC 9110 - HTTP Semantics]] (section ''10.2.2. Location'')
   */
  def getRedirectUrl(response: HttpResponse, url: String): Option[String] = {
    response.header[Location].map(location => {
      var redirectUri = location.uri

      if (redirectUri.isRelative) {
        val originalUri = Uri(url)
        redirectUri = redirectUri.resolvedAgainst(originalUri).withFragment(originalUri.fragment.orNull)
      }

      redirectUri.toString
    })
  }
}
