package abwcf.actors.persistence.host

import abwcf.data.HostInformation

/**
 * The protocol of [[abwcf.actors.persistence.host]] actors.
 */
object HostPersistence {
  sealed trait Command

  sealed trait InsertCommand extends Command
  case class Insert(hostInfo: HostInformation) extends InsertCommand

  sealed trait ReadCommand extends Command
  case class Recover(schemeAndAuthority: String) extends ReadCommand
  
  sealed trait UpdateCommand extends Command
  case class Update(hostInfo: HostInformation) extends UpdateCommand
}
