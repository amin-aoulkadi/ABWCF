package abwcf.util

case class CrawlerSettings(userCode: UserCode = new UserCode {}) {
  def withUserCode(userCode: UserCode): CrawlerSettings = copy(userCode = userCode)
}
