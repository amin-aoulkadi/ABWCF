package abwcf.actors

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.ShardingEnvelope
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import org.apache.pekko.persistence.typed.{PersistenceId, RecoveryCompleted}

import java.net.URI

/**
 * Represents a page to be crawled.
 *
 * There should be exactly one [[Page]] actor per page. [[Page]] actors should be managed by a [[PageManager]] actor.
 *
 * This actor is stateful, sharded and event sourced.
 *
 * Entity ID: URL of the page.
 */
object Page {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("Page")

  sealed trait Command
  case class Discover(url: String, crawlDepth: Int) extends Command
  case object Success extends Command
  case object Redirect extends Command
  case object Error extends Command

  sealed trait Event
  case class Discovered(url: String, crawlDepth: Int) extends Event
  case object Processed extends Event

  sealed trait State {
    def applyCommand(command: Command): Effect[Event, State]
    def applyEvent(event: Event): State //Only updates the state and must not have side effects.
  }

  case object UnknownPage extends State {
    override def applyCommand(command: Command): Effect[Event, State] = command match {
      case Discover(url, crawlDepth) => Effect.persist(Discovered(url, crawlDepth))
      case _ => Effect.unhandled
    }

    override def applyEvent(event: Event): State = event match {
      case Discovered(url, crawlDepth) => DiscoveredPage(url, crawlDepth)
      case _ => throw new IllegalStateException(s"Unexpected event $event in state $this")
    }
  }

  case class DiscoveredPage(url: String, crawlDepth: Int) extends State {
    override def applyCommand(command: Command): Effect[Event, State] = command match {
      case Discover(_, _) => Effect.none //The crawler can discover the same page multiple times, but it doesn't need to fetch the same page multiple times.
      case Success | Redirect | Error => Effect.persist(Processed)
    }

    override def applyEvent(event: Event): State = event match {
      case Processed => ProcessedPage
      case _ => throw new IllegalStateException(s"Unexpected event $event in state $this")
    }
  }

  case object ProcessedPage extends State{
    override def applyCommand(command: Command): Effect[Event, State] = {
      Effect.none
    }

    override def applyEvent(event: Event): State = {
      throw new IllegalStateException(s"Unexpected event $event in state $this")
    }
  }

  def apply(url: String): Behavior[Command] = Behaviors.setup(context => {
    val hostQueueShardRegion = HostQueue.getShardRegion(context.system)

    EventSourcedBehavior[Command, Event, State](
      PersistenceId(TypeKey.name, url),
      emptyState = UnknownPage,
      commandHandler = (state, command) => state.applyCommand(command),
      eventHandler = (state, event) => state.applyEvent(event)
    )
      .receiveSignal({
        case (state, RecoveryCompleted) if state != ProcessedPage => //RecoveryCompleted is sent to both recovered actors and newly created actors.
          //Add the page to a HostQueue so that it can be fetched:
          val host = URI(url).getHost
          hostQueueShardRegion ! ShardingEnvelope(host, HostQueue.Enqueue(url))
      })
  })

  def getShardRegion(system: ActorSystem[?]): ActorRef[ShardingEnvelope[Command]] = {
    ClusterSharding(system).init(Entity(TypeKey)(entityContext => Page(entityContext.entityId)))
  }
}
