package abwcf.services.robots_tag

import java.util.Locale

/**
 * Represents (part of) a string that is currently being processed by a [[RobotsMetaParsingService]], along with additional information that is required by [[DirectiveParser]] and [[RobotsMetaParsingService]] instances.
 *
 * @param string the string
 * @param firstToken The first token in the string (i.e. everything up to the first delimiter), all lowercase and trimmed. It can be ambiguous whether this token is a directive name or a user agent.
 * @param delimiterIndex the index of the first delimiting character in the string, or `string.length` if the string contains no delimiters
 * @param delimiter The first delimiting character in the string. Can be a comma, a colon or [[None]].
 * @param tail everything after the first delimiter (trimmed), or [[None]] if there is no delimiter or if there are no characters after the delimiter
 */
case class PreprocessedString(
                               string: String,
                               firstToken: String,
                               delimiterIndex: Int,
                               delimiter: Option[Char],
                               tail: Option[String]
                             )

object PreprocessedString {
  def apply(string: String, firstToken: String, delimiterIndex: Int, delimiter: Char, tail: String): PreprocessedString =
    new PreprocessedString(string, firstToken, delimiterIndex, Option(delimiter), Option(tail))

  /**
   * Preprocesses a string to create a new [[PreprocessedString]].
   */
  def apply(string: String): PreprocessedString = {
    //Find the first comma or colon:
    var delimiter: Option[Char] = None
    var delimiterIndex = 0

    while (delimiter.isEmpty && delimiterIndex < string.length) {
      val char = string.charAt(delimiterIndex)

      if (char == ',' || char == ':') {
        delimiter = Some(char)
      } else {
        delimiterIndex += 1
      }
    }

    //Retrieve and normalize the first token:
    val firstToken = string.substring(0, delimiterIndex)
      .trim
      .toLowerCase(Locale.ROOT)

    //Retrieve and normalize the tail:
    var tail: Option[String] = None

    if (delimiterIndex < string.length) {
      val substring = string.substring(delimiterIndex + 1).trim

      if (substring.nonEmpty) {
        tail = Some(substring)
      }
    }

    new PreprocessedString(string, firstToken, delimiterIndex, delimiter, tail)
  }
}
