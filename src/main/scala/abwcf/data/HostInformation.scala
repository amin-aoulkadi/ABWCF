package abwcf.data

import crawlercommons.robots.SimpleRobotRules

import java.time.Instant

/**
 * Host-specific information that is relevant for polite crawling (e.g. rules from `robots.txt`).
 *
 * Here, a host is identified by the scheme and authority components of a URL. This is because a `robots.txt` file applies to a (scheme, authority) tuple.
 *
 * @example The rules defined in `https://example.com:1234/robots.txt` only apply to resources that are hosted under the same scheme (i.e. `https`) and authority (i.e. `example.com:1234`). They do not apply to any other resources hosted by `example.com`.
 */
case class HostInformation(schemeAndAuthority: String, robotRules: SimpleRobotRules, validUntil: Instant) {
  def isExpired: Boolean =
    Instant.now.isAfter(validUntil)
}
