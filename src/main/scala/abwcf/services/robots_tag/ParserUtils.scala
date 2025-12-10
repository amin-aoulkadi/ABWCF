package abwcf.services.robots_tag

object ParserUtils {
  /**
   * Returns the index of the first comma in the string, or `string.length` if the string contains no commas.
   */
  def findFirstComma(string: String): Int = {
    string.indexOf(',') match {
      case -1 => string.length //The string does not contain any commas.
      case i => i
    }
  }
}
