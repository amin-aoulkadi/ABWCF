package abwcf.actors.metrics

import abwcf.api.CrawlerSettings
import abwcf.metrics.FetcherMetrics
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.model.HttpResponse

object FetcherMetricsAggregator {
  sealed trait Command
  case class AddFetchedPages(value: Long) extends Command
  case class AddResponse(response: HttpResponse) extends Command
  case class AddReceivedBytes(value: Long) extends Command
  case class AddException(exception: Throwable) extends Command

  def apply(settings: CrawlerSettings): Behavior[Command] = Behaviors.setup(context => {
    val metrics = FetcherMetrics(settings, context)

    Behaviors.receiveMessage({
      case AddFetchedPages(value) =>
        metrics.addFetchedPages(value)
        Behaviors.same

      case AddResponse(response) =>
        metrics.addResponse(response)
        Behaviors.same

      case AddReceivedBytes(value) =>
        metrics.addReceivedBytes(value)
        Behaviors.same

      case AddException(exception) =>
        metrics.addException(exception)
        Behaviors.same
    })
  })
}
