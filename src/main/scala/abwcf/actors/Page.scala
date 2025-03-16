package abwcf.actors

import abwcf.actors.Page.Status.{Discovered, Undefined}
import org.apache.pekko.actor.typed.receptionist.Receptionist
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, StashBuffer, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, receptionist}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.util.ByteString

import java.net.URI
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Represents a page to be crawled.
 *
 * There should be exactly one [[Page]] actor per page. [[Page]] actors should be managed by a [[PageManager]] actor.
 *
 * This actor is stateful and sharded.
 *
 * Entity ID: URL of the page.
 */
object Page {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("Page")

  enum Status:
    case Undefined, Discovered
  
  sealed trait Command
  case object Discover extends Command
  case class FetchSuccess(response: HttpResponse, responseBody: ByteString) extends Command //TODO: Probably better to use a custom class instead of HttpResponse.
  case object FetchFailure extends Command
  private case object RetrySetup extends Command

  private type CombinedCommand = Command | Receptionist.Listing
  
  def apply(url: String): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    Behaviors.withStash(100)(buffer => {
      Behaviors.withTimers(timers => {
        val hostQueueShardRegion = HostQueue.getShardRegion(context.system)
        new Page(url, hostQueueShardRegion, context, buffer, timers).setup()
      })
    })
  }).narrow

  def getShardRegion(system: ActorSystem[?]): ActorRef[ShardingEnvelope[Command]] = {
    ClusterSharding(system).init(Entity(TypeKey)(entityContext => Page(entityContext.entityId)))
  }
}

private class Page private (url: String,
                            hostQueueShardRegion: ActorRef[ShardingEnvelope[HostQueue.Command]],
                            context: ActorContext[Page.CombinedCommand],
                            buffer: StashBuffer[Page.CombinedCommand],
                            timers: TimerScheduler[Page.CombinedCommand]) {
  import Page.*

  private def setup(): Behavior[CombinedCommand] = {
    //Query the receptionist for UserCodeRunners:
    context.system.receptionist ! Receptionist.Find(UserCodeRunner.UCRServiceKey, context.self)

    //Handle the reply from the receptionist:
    Behaviors.receiveMessage({
      case UserCodeRunner.UCRServiceKey.Listing(actors) =>
        actors.find(_.path.address.hasLocalScope) match {
          case Some(userCodeRunner) =>
            buffer.unstashAll(page(Undefined, userCodeRunner))
            
          case None =>
            //Try again later:
            if (context.system.uptime < 60) {
              context.log.info("Waiting for a local UserCodeRunner")
              timers.startSingleTimer(RetrySetup, 5 seconds)
            } else {
              context.log.error("Failed to find a local UserCodeRunner")
              timers.startSingleTimer(RetrySetup, 30 seconds)
            }
            
            Behaviors.same
        }

      case RetrySetup =>
        setup()

      case other =>
        if !buffer.contains(other) then buffer.stash(other)
        Behaviors.same
    })
  }
  
  private def page(status: Status, userCodeRunner: ActorRef[UserCodeRunner.Command]): Behavior[CombinedCommand] = Behaviors.receiveMessage({
    case Discover if status == Undefined =>
      //Add the page to a HostQueue so that it can be fetched:
      val host = URI(url).getHost
      hostQueueShardRegion ! ShardingEnvelope(host, HostQueue.Enqueue(url))
      page(Discovered, userCodeRunner)

    case Discover => //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.
      Behaviors.same

    case FetchSuccess(response, responseBody) =>
      //TODO: Page status and behavior should change once it has been fetched.
      userCodeRunner ! UserCodeRunner.ProcessPage(url, response, responseBody)
      Behaviors.same

    case FetchFailure =>
      //TODO: Handle fetch failure.
      Behaviors.same

    case other =>
      context.log.info("Skipping unexpected message {}", other)
      Behaviors.same
  })
}
