package abwcf.persistence

import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.connectors.slick.scaladsl.SlickSession

import scala.concurrent.Future

object CoordinatedSlickSession {
  /**
   * Creates a new [[SlickSession]] that is closed automatically when the actor system terminates.
   */
  def create(system: ActorSystem[?]): SlickSession = {
    val session = SlickSession.forConfig("abwcf.persistence.slick-config")

    CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "close-database-resources")(() => {
      Future({
        system.log.info("Closing database resources")
        session.close()
        Done
      })(using system.executionContext)
    })

    session
  }
}
