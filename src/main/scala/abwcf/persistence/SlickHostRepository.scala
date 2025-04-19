package abwcf.persistence

import abwcf.data.HostInformation
import crawlercommons.robots.SimpleRobotRules
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.slick.scaladsl.{Slick, SlickSession}
import org.apache.pekko.stream.scaladsl.Sink

import java.time.Instant
import java.util.stream.Collectors
import scala.concurrent.Future

class SlickHostRepository(implicit val session: SlickSession, val materializer: Materializer) extends HostRepository {
  import session.profile.api.*

  private def hostToTuple(hostInfo: HostInformation): (String, String, Long, Instant) = {
    val ruleString = hostInfo.robotRules.getRobotRules.stream()
      .map(ruleToString)
      .collect(Collectors.joining("\n"))

    (hostInfo.schemeAndAuthority, ruleString, hostInfo.robotRules.getCrawlDelay, hostInfo.validUntil)
  }

  private def tupleToHost(tuple: (String, String, Long, Instant)): HostInformation = {
    val rules = new SimpleRobotRules()
    rules.setCrawlDelay(tuple._3)
    tuple._2.split('\n')
      .map(stringToRule)
      .foreach((prefix, isAllow) => rules.addRule(prefix, isAllow))

    HostInformation(tuple._1, rules, tuple._4)
  }

  private def ruleToString(rule: SimpleRobotRules.RobotRule): String = {
    if (rule.isAllow) {
      s"allow: ${rule.getPrefix}"
    } else {
      s"disallow: ${rule.getPrefix}"
    }
  }

  private def stringToRule(string: String): (String, Boolean) = string match {
    case s"allow: $prefix" => (prefix, true)
    case s"disallow: $prefix" => (prefix, false)
  }

  private class HostTable(tag: Tag) extends Table[HostInformation](tag, None, "hosts") {
    def schemeAndAuthority = column[String]("scheme_and_authority")
    def robotRules = column[String]("robot_rules")
    def crawlDelay = column[Long]("crawl_delay")
    def validUntil = column[Instant]("valid_until")

    override def * = (schemeAndAuthority, robotRules, crawlDelay, validUntil) <> (tupleToHost, hostToTuple)
  }

  private lazy val hosts = TableQuery[HostTable]
  private lazy val compiledHosts = Compiled(hosts.map(identity)) //Based on https://stackoverflow.com/a/50014513.

  private lazy val compiledUpdate = Compiled((schemeAndAuthority: Rep[String], robotRules: Rep[String], crawlDelay: Rep[Long], validUntil: Rep[Instant]) => {
    hosts
      .filter(_.schemeAndAuthority === schemeAndAuthority)
      .map(h => (h.schemeAndAuthority, h.robotRules, h.crawlDelay, h.validUntil))
  })
  
  private lazy val compiledSelect = Compiled((schemeAndAuthority: Rep[String]) => {
    hosts.filter(_.schemeAndAuthority === schemeAndAuthority)
  })

  override def insert(hostInfo: HostInformation): Future[Int] = {
    session.db.run(compiledHosts += hostInfo)
  }

  override def update(hostInfo: HostInformation): Future[Int] = {
    val tuple = hostToTuple(hostInfo)
    session.db.run(compiledUpdate(tuple).update(tuple)) //Based on https://stackoverflow.com/a/38261000.
  }
  
  override def findBySchemeAndAuthority(schemeAndAuthority: String): Future[Option[HostInformation]] = {
    Slick.source(compiledSelect(schemeAndAuthority).result).runWith(Sink.headOption)
  }
}
