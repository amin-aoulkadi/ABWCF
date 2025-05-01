package abwcf.actors

import abwcf.data.Page
import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey as ReceptionistServiceKey}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import org.apache.pekko.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}

import java.time.{Duration, Instant}
import scala.collection.mutable
import scala.compiletime.uninitialized
import scala.jdk.DurationConverters.*

/**
 * Manages the crawl delay for a host and sorts pages by crawl priority.
 *
 * There should be exactly one [[HostQueue]] actor per crawled host.
 *
 * This actor is stateful, sharded, gracefully passivated and registered with the receptionist.
 *
 * Entity ID: Scheme and authority components of a URL.
 *
 * This entity will not be remembered (even if remembering entities is enabled).
 */
object HostQueue {
  val TypeKey: EntityTypeKey[Command] = EntityTypeKey("HostQueue")
  val ServiceKey: ReceptionistServiceKey[Command] = ReceptionistServiceKey("HostQueue")

  sealed trait Command
  case class Enqueue(page: Page) extends Command
  case class GetHead(replyTo: ActorRef[Reply]) extends Command
  private case object Passivate extends Command

  sealed trait Reply
  case class Head(page: Page) extends Reply
  case class Unavailable(until: Instant) extends Reply

  private type CombinedCommand = Command | HostManager.HostInfo

  def apply(entityContext: EntityContext[Command]): Behavior[Command] = Behaviors.setup[CombinedCommand](context => {
    new HostQueue(entityContext.entityId, entityContext.shard, context).requestHostInfo()
  }).narrow

  def initializeSharding(system: ActorSystem[?]): ActorRef[ShardingEnvelope[Command]] = {
    val settings = ClusterShardingSettings(system)
      .withRememberEntities(false) //HostQueue state is not persisted, so it doesn't make sense to remember HostQueue entities.
      .withNoPassivationStrategy() //Disable automatic passivation.

    ClusterSharding(system).init(
      Entity(TypeKey)(entityContext => HostQueue(entityContext))
        .withSettings(settings)
    )
  }
}

private class HostQueue private (schemeAndAuthority: String,
                                 shard: ActorRef[ClusterSharding.ShardCommand],
                                 context: ActorContext[HostQueue.CombinedCommand]) {
  import HostQueue.*

  private val sharding = ClusterSharding(context.system)
  private val config = context.system.settings.config
  private val receiveTimeout = config.getDuration("abwcf.actors.host-queue.passivation-receive-timeout").toScala

  /**
   * Contains [[Page]]s sorted by crawl priority.
   */
  private val queue = mutable.PriorityQueue.empty[Page](using Ordering.by(_.crawlPriority)) //Mutable state!

  /**
   * Contains the URLs of the [[Page]]s that are in the queue.
   *
   * This is required to avoid enqueueing the same page twice. Consider the following scenario:
   *  1. Enqueue page ''p''.
   *  1. The [[PageManager]] for ''p'' is passivated.
   *  1. The [[PageManager]] for ''p'' is restored by the [[PageRestorer]]; ''p'' has not been fetched yet.
   *  1. The [[PageManager]] for ''p'' tells the [[HostQueue]] to enqueue ''p''.
   *
   * @note Other data structures were also considered for this purpose:
   *       - [[mutable.TreeSet]] - Unsuitable because it detects duplicate elements based on equality according to the [[Ordering]] (i.e. it considers two pages as equal if they have the same crawl priority, even if their URLs are distinct).
   *       - [[mutable.TreeMap]] - Detects duplicate keys based on equality according to the [[Ordering]] (similar to `TreeSet`). Would only be suitable if it were used as `TreeMap[Long, Set[Page]]`.
   */
  private val urlSet = mutable.HashSet.empty[String] //Mutable state!

  private var crawlDelay: Duration = uninitialized //Mutable state!
  private var crawlDelayEnd = Instant.MIN //Mutable state!

  private def requestHostInfo(): Behavior[CombinedCommand] = {
    //Request information from the HostManager:
    val hostManager = sharding.entityRefFor(HostManager.TypeKey, schemeAndAuthority)
    hostManager ! HostManager.GetHostInfo(context.self) //TODO: Better use ask so that there is a timeout and retry.

    Behaviors.receiveMessage({
      case HostManager.HostInfo(hostInfo) => //TODO: Handle HostInformation expiry.
        crawlDelay = Duration.ofMillis(hostInfo.robotRules.getCrawlDelay)

        if (queue.isEmpty) {
          emptyQueue()
        } else {
          nonEmptyQueue()
        }

      case Enqueue(page) =>
        enqueue(page)
        Behaviors.same

      case GetHead(replyTo) =>
        replyTo ! Unavailable(Instant.MAX)
        Behaviors.same

      case Passivate =>
        context.log.warn("Skipping unexpected message {}", Passivate)
        Behaviors.same
    })
  }

  private def nonEmptyQueue(): Behavior[CombinedCommand] = {
    //Disable passivation and register with the receptionist:
    context.cancelReceiveTimeout() //Non-empty HostQueues should not be passivated.
    context.system.receptionist ! Receptionist.Register(ServiceKey, context.self) //Allows the HostQueueRouter to route messages to this HostQueue.

    Behaviors.receiveMessage({
      case Enqueue(page) =>
        enqueue(page)
        Behaviors.same

      case GetHead(replyTo) if Instant.now.isAfter(crawlDelayEnd) =>
        val head = dequeue()
        replyTo ! Head(head)
        crawlDelayEnd = Instant.now.plus(crawlDelay)

        if (queue.isEmpty) {
          context.system.receptionist ! Receptionist.Deregister(ServiceKey, context.self) //The HostQueueRouter should stop routing messages to this HostQueue.
          emptyQueue()
        } else {
          Behaviors.same
        }

      case GetHead(replyTo) =>
        replyTo ! Unavailable(crawlDelayEnd)
        Behaviors.same

      case other =>
        context.log.warn("Skipping unexpected message {}", other)
        Behaviors.same
    })
  }

  private def emptyQueue(): Behavior[CombinedCommand] = {
    context.setReceiveTimeout(receiveTimeout, Passivate) //Enable passivation. Empty HostQueues can be passivated (but not immediately as there may still be messages in the mailbox).

    Behaviors.receiveMessage({
      case Enqueue(page) =>
        enqueue(page)
        nonEmptyQueue()

      case GetHead(replyTo) =>
        replyTo ! Unavailable(Instant.MAX)
        Behaviors.same

      case Passivate =>
        shard ! ClusterSharding.Passivate(context.self)
        Behaviors.same

      case other =>
        context.log.warn("Skipping unexpected message {}", other)
        Behaviors.same
    })
  }

  /**
   * Adds a [[Page]] to the queue if it is not in the queue yet.
   */
  private def enqueue(page: Page): Unit = {
    if (!urlSet.contains(page.url)) {
      queue.enqueue(page)
      urlSet.add(page.url)
    }
  }

  /**
   * Returns the [[Page]] with the highest crawl priority in the queue and removes it from the queue.
   */
  private def dequeue(): Page = {
    val head = queue.dequeue()
    urlSet.remove(head.url)
    head
  }
}
