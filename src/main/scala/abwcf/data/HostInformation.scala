package abwcf.data

import crawlercommons.robots.SimpleRobotRules

import java.time.Instant

case class HostInformation(schemeAndAuthority: String, robotRules: SimpleRobotRules, validUntil: Instant)
