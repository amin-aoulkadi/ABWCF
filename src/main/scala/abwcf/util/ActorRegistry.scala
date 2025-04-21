package abwcf.util

import abwcf.actors.persistence.host.HostPersistence
import org.apache.pekko.actor.typed.ActorRef

/**
 * Contains [[ActorRef]]s that can be accessed anywhere in the application.
 * 
 * '''Caution:''' It is possible to create multiple actor systems in one JVM, but there can only be one [[ActorRegistry]] per JVM.
 */
object ActorRegistry {
  var hostPersistenceManager: Option[ActorRef[HostPersistence.Command]] = None
}
