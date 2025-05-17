package abwcf.actors.persistence

import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
 * Builds batches and sends them to a batch consumer.
 *
 * This actor is stateful.
 */
object Batcher {
  sealed trait Command
  case class Add[T](element: T) extends Command
  private case object SendBatch extends Command

  case class Batch[T](batch: Seq[T])

  def apply[T](maxBatchSize: Int, maxBatchDelay: FiniteDuration, batchConsumer: ActorRef[Batch[T]]): Behavior[Command] = Behaviors.setup(context => {
    Behaviors.withTimers(timers => {
      new Batcher(maxBatchSize, maxBatchDelay, batchConsumer, context, timers).batcher()
    })
  })
}

private class Batcher[T] private(maxBatchSize: Int,
                                 maxBatchDelay: FiniteDuration,
                                 batchConsumer: ActorRef[Batcher.Batch[T]],
                                 context: ActorContext[Batcher.Command],
                                 timers: TimerScheduler[Batcher.Command]) {
  import Batcher.*

  private val batch = mutable.ListBuffer.empty[T] //Mutable state!

  private def batcher(): Behavior[Command] = Behaviors.receiveMessage({
    case Add(element: T @unchecked) if batch.isEmpty =>
      batch.append(element)
      timers.startSingleTimer(SendBatch, maxBatchDelay) //Ensure that the batch is sent after maxBatchDelay.
      Behaviors.same

    case Add(element: T @unchecked) =>
      batch.append(element)

      if (batch.size >= maxBatchSize) { //The batch is full.
        sendBatch()
      }

      Behaviors.same

    case SendBatch =>
      sendBatch()
      Behaviors.same
  })

  private def sendBatch(): Unit = {
    batchConsumer ! Batch(batch.toList)
    timers.cancelAll() //Ensure that the new batch does not receive the SendBatch message of the previous batch.
    batch.clear()
  }
}
