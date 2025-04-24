package abwcf.persistence

import abwcf.data.HostInformation
import crawlercommons.robots.SimpleRobotRules
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.slick.scaladsl.{Slick, SlickSession}
import org.apache.pekko.stream.scaladsl.Sink
import slick.jdbc.GetResult

import java.sql.Timestamp
import java.util.stream.Collectors
import scala.concurrent.Future

class SlickHostRepository(using session: SlickSession, materializer: Materializer) extends HostRepository {
  import session.profile.api.*

  /**
   * Converts a result set to a [[HostInformation]] instance.
   */
  private given getHostInformationResult: GetResult[HostInformation] = GetResult(result => {
    val schemeAndAuthority = result.nextString()
    val rules = result.nextString()
    val crawlDelay = result.nextLong()
    val validUntil = result.nextTimestamp().toInstant

    val robotRules = new SimpleRobotRules()
    robotRules.setCrawlDelay(crawlDelay)

    if (rules.nonEmpty) {
      rules.split('\n')
        .map({
          case s"allow: $prefix" => (prefix, true)
          case s"disallow: $prefix" => (prefix, false)
        })
        .foreach((prefix, isAllow) => robotRules.addRule(prefix, isAllow))
    }

    HostInformation(schemeAndAuthority, robotRules, validUntil)
  })

  private def rulesToString(rules: SimpleRobotRules): String = {
    rules.getRobotRules.stream()
      .map({
        case rule if rule.isAllow => s"allow: ${rule.getPrefix}"
        case rule if !rule.isAllow => s"disallow: ${rule.getPrefix}"
      })
      .collect(Collectors.joining("\n"))
  }

  override def insert(hostInfo: HostInformation): Future[Int] = {
    val schemeAndAuthority = hostInfo.schemeAndAuthority
    val robotRules = rulesToString(hostInfo.robotRules)
    val crawlDelay = hostInfo.robotRules.getCrawlDelay
    val validUntil = Timestamp.from(hostInfo.validUntil)

    val query = sqlu"""INSERT INTO hosts VALUES ($schemeAndAuthority, $robotRules, $crawlDelay, $validUntil)"""
    session.db.run(query)
  }

  override def update(hostInfo: HostInformation): Future[Int] = {
    val schemeAndAuthority = hostInfo.schemeAndAuthority
    val robotRules = rulesToString(hostInfo.robotRules)
    val crawlDelay = hostInfo.robotRules.getCrawlDelay
    val validUntil = Timestamp.from(hostInfo.validUntil)

    val query = sqlu"""UPDATE hosts SET robot_rules = $robotRules, crawl_delay = $crawlDelay, valid_until = $validUntil WHERE scheme_and_authority = $schemeAndAuthority"""
    session.db.run(query)
  }

  override def findBySchemeAndAuthority(schemeAndAuthority: String): Future[Option[HostInformation]] = {
    val query = sql"""SELECT * FROM hosts WHERE scheme_and_authority = $schemeAndAuthority""".as[HostInformation]
    Slick.source(query).runWith(Sink.headOption)
  }
}
