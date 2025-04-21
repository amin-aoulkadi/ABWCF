package abwcf.persistence

import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.connectors.slick.scaladsl.SlickSession

import scala.concurrent.Future

/**
 * Contains a singleton [[SlickSession]] that can be accessed anywhere in the application.
 *
 * The session is closed automatically when the actor system terminates.
 *
 * '''Caution:''' It is possible to create multiple actor systems in one JVM, but there can only be one [[SlickSessionContainer]] per JVM.
 */
object SlickSessionContainer {
  private var session: Option[SlickSession] = None

  /**
   * Initializes the session based on the [[com.typesafe.config.Config]] of the actor system.
   */
  def initialize(system: ActorSystem[?]): Unit = {
    if (session.isEmpty) {
      val slickSession = SlickSession.forConfig("abwcf.persistence.slick-config")
      session = Some(slickSession)

      CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "close-database-connections")(() => {
        Future({
          system.log.info("Closing database connections")
          slickSession.close()
          Done
        })(using system.executionContext)
      })
    }
  }

  /**
   * @throws NoSuchElementException if the session is uninitialized
   */
  def getSession: SlickSession =
    session.get
}
