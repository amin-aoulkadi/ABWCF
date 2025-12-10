package abwcf.services.robots_tag

object ExceptionHandlers {
  /**
   * Throws all exceptions.
   */
  def throwing[T <: Exception](exception: T): Unit =
    throw exception

  /**
   * Ignores all exceptions and does nothing.
   */
  def ignoring[T <: Exception](exception: T): Unit = ()
}
