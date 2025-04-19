package abwcf.actors.persistence.page

import abwcf.data.{Page, PageStatus}
import org.apache.pekko.actor.typed.ActorRef

/**
 * The protocol of [[abwcf.actors.persistence.page]] actors.
 */
object PagePersistence {
  sealed trait Command
  
  sealed trait InsertCommand extends Command
  case class Insert(page: Page) extends InsertCommand
  
  sealed trait ReadCommand extends Command
  case class FindByStatus(status: PageStatus, limit: Int, replyTo: ActorRef[ResultSeq]) extends ReadCommand
  case class Recover(url: String) extends ReadCommand
  
  sealed trait UpdateCommand extends Command
  case class UpdateStatus(url: String, status: PageStatus) extends UpdateCommand

  sealed trait Reply
  case class ResultSeq(result: Seq[Page]) extends Reply
}
