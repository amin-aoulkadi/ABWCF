package abwcf.util

import java.net.URI

object UrlUtils {
  /**
   * Returns the scheme and authority components of a URL.
   *
   * @example `https://www.example.com`
   * @example `http://user@example.com:1234`
   */
  def getSchemeAndAuthority(url: String): String = {
    val uri = new URI(url)
    s"${uri.getScheme}://${uri.getRawAuthority}"
  }
}
