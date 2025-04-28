package abwcf.util

import io.opentelemetry.api.OpenTelemetry

case class CrawlerSettings(
                            userCode: UserCode = new UserCode {},
                            openTelemetry: OpenTelemetry = OpenTelemetry.noop()
                          ) {
  def withUserCode(userCode: UserCode): CrawlerSettings =
    copy(userCode = userCode)

  def withOpenTelemetry(openTelemetry: OpenTelemetry): CrawlerSettings =
    copy(openTelemetry = openTelemetry)
}
