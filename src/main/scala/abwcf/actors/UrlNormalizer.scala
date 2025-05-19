package abwcf.actors

import abwcf.data.PageCandidate
import abwcf.metrics.UrlNormalizerMetrics
import abwcf.util.CrawlerSettings
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import java.net.{IDN, URI}
import java.util.Locale

/**
 * Normalizes URLs and removes user information, query and fragment URL components as configured.
 *
 * There should be one [[UrlNormalizer]] actor per node.
 *
 * This actor is stateless.
 *
 * @note URL normalization is surprisingly difficult to implement because some of the getters and multi-argument constructors of [[URI]] mess with percent-encoded characters in various ways. A different URL parser could be useful.
 * @see
 *      - [[https://datatracker.ietf.org/doc/html/rfc3986 RFC 3986 - Uniform Resource Identifier (URI): Generic Syntax]]
 *      - [[https://en.wikipedia.org/wiki/URI_normalization Wikipedia: URI normalization]]
 *      - [[https://en.wikipedia.org/wiki/Percent-encoding Wikipedia: Percent-encoding]]
 */
object UrlNormalizer {
  sealed trait Command
  case class Normalize(candidate: PageCandidate) extends Command

  def apply(urlFilter: ActorRef[UrlFilter.Command], settings: CrawlerSettings): Behavior[Command] = Behaviors.setup(context => {
    val config = context.system.settings.config
    val removeUserInfo = config.getBoolean("abwcf.actors.url-normalizer.remove-userinfo")
    val removeQuery = config.getBoolean("abwcf.actors.url-normalizer.remove-query")
    val removeFragment = config.getBoolean("abwcf.actors.url-normalizer.remove-fragment")
    val logExceptions = config.getBoolean("abwcf.actors.url-normalizer.log-exceptions")
    val metrics = UrlNormalizerMetrics(settings, context)

    Behaviors.receiveMessage({
      case Normalize(candidate) =>
        try {
          //Normalize and remove URL components as configured:
          val uri = URI(candidate.url).normalize()
          val scheme = uri.getScheme.toLowerCase(Locale.ROOT)
          val userInfo = if removeUserInfo then null else uri.getRawUserInfo
          val host = normalizeHost(getHost(uri))
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

          //Ensure that the normalized URL is a valid-ish URL:
          val normalized = URI(builder.toString()).parseServerAuthority()

          urlFilter ! UrlFilter.Filter(candidate.copy(url = normalized.toString))
        } catch {
          case e: Exception =>
            if logExceptions then context.log.error("Exception while normalizing URL {}", candidate.url, e)
            metrics.addException(e)
        }

        metrics.addProcessedUrls(1)
        Behaviors.same
    })
  })

  @throws[NullPointerException]("if uri.getHost and uri.getRawAuthority both return null")
  private def getHost(uri: URI): String = uri.getHost match {
      case null => //This happens if the host contains non-ASCII characters.
        uri.getRawAuthority //"user@👀.example"
          .split('@') //["user", "👀.example"]
          .last

      case host => host
  }

  /**
   * Normalizes the host component.
   *
   * Unicode characters (e.g. from internationalized domain names) are converted to ASCII.
   *
   * @see
   *      - [[https://datatracker.ietf.org/doc/html/rfc3490 RFC 3490 - Internationalizing Domain Names in Applications (IDNA)]] (initial specification; obsoleted by RFC 5890 and RFC 5891)
   *      - [[https://datatracker.ietf.org/doc/html/rfc5890 RFC 5890 - Internationalized Domain Names for Applications (IDNA): Definitions and Document Framework]]
   *      - [[https://datatracker.ietf.org/doc/html/rfc5891 RFC 5891 - Internationalized Domain Names in Applications (IDNA): Protocol]]
   *      - [[https://en.wikipedia.org/wiki/Internationalized_domain_name Wikipedia: Internationalized domain name]]
   */
  @throws[IllegalArgumentException]
  private def normalizeHost(host: String): String = {
    IDN.toASCII(host, IDN.ALLOW_UNASSIGNED) //IDN.ALLOW_UNASSIGNED to enable support for Unicode characters that were added after Unicode 3.2 (released in 2002).
      .toLowerCase(Locale.ROOT)
  }

  /**
   * Normalizes the port if it is the default port for the given scheme.
   */
  private def normalizePort(port: Int, scheme: String): Int = (port, scheme) match {
    case (80, "http") => -1
    case (443, "https") => -1
    case _ => port
  }
}
