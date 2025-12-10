package abwcf.services.robots_tag

class ParserException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) =
    this(message, null)
}
