package abwcf.api

import io.opentelemetry.api.{GlobalOpenTelemetry, OpenTelemetry}

case class CrawlerSettings(
                            userCode: UserCode = new UserCode {},
                            openTelemetry: OpenTelemetry = GlobalOpenTelemetry.getOrNoop()
                          ) {
  def withUserCode(userCode: UserCode): CrawlerSettings =
    copy(userCode = userCode)

  def withOpenTelemetry(openTelemetry: OpenTelemetry): CrawlerSettings =
    copy(openTelemetry = openTelemetry)
}
