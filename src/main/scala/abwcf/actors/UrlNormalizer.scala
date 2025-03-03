package abwcf.actors

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import java.net.{URI, URISyntaxException}
import java.util.Locale

/**
 * Normalizes URLs and removes user information, query and fragment URL components as configured.
 *
 * This actor is stateless.
 *
 * @note URL normalization is surprisingly difficult to implement because some of the getters and multi-argument constructors of [[URI]] mess with percent-encoded characters in various ways.
 * @see
 *      - [[https://datatracker.ietf.org/doc/html/rfc3986 RFC 3986 - Uniform Resource Identifier (URI): Generic Syntax]]
 *      - [[https://en.wikipedia.org/wiki/URI_normalization Wikipedia: URI normalization]]
 *      - [[https://en.wikipedia.org/wiki/Percent-encoding Wikipedia: Percent-encoding]]
 */
object UrlNormalizer {
  sealed trait Command
  case class Normalize(url: String) extends Command

  sealed trait Result
  case class NormalizedUrl(url: String) extends Result

  @throws[URISyntaxException]
  def apply(sendTo: ActorRef[Result]): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val removeUserInfo = config.getBoolean("abwcf.url-normalizer.remove-userinfo")
    val removeQuery = config.getBoolean("abwcf.url-normalizer.remove-query")
    val removeFragment = config.getBoolean("abwcf.url-normalizer.remove-fragment")

    Behaviors.receiveMessage({
      case Normalize(urlString) =>
        //Normalize and remove URL components as configured:
        val uri = URI(urlString).normalize
        val scheme = uri.getScheme.toLowerCase(Locale.ROOT)
        val userInfo = if removeUserInfo then null else uri.getRawUserInfo
        val host = uri.getHost.toLowerCase(Locale.ROOT)
        val port = normalizePort(uri.getPort, scheme)
        val path = if uri.getRawPath.isEmpty then "/" else uri.getRawPath
        val query = if removeQuery then null else uri.getRawQuery
        val fragment = if removeFragment then null else uri.getRawFragment

        //Assemble the normalized URL (the URI class can not be used for this due to issues with percent-encoding):
        val builder = StringBuilder(scheme) ++= "://"
        if userInfo != null then builder ++= userInfo += '@'
        builder ++= host
        if port >= 0 then (builder += ':').append(port)
        builder ++= path
        if query != null then builder += '?' ++= query
        if fragment != null then builder += '#' ++= fragment

        sendTo ! NormalizedUrl(builder.toString)
        Behaviors.same
    })
  })

  /**
   * Normalizes the port if it is the default port for the given scheme.
   */
  private def normalizePort(port: Int, scheme: String): Int = (port, scheme) match {
    case (80, "http") => -1
    case (443, "https") => -1
    case _ => port
  }
}
