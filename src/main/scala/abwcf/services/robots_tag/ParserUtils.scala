package abwcf.services.robots_tag

import java.util.Locale
import scala.util.matching.Regex

object ParserUtils {
  /**
   * Removes the first token and everything up to the first regular expression match from a [[PreprocessedString]].
   * 
   * Returns an empty string if the regular expression finds no match.
   */
  def dropUntilFirstMatch(regex: Regex, preprocessed: PreprocessedString): String = {
    preprocessed.tail match {
      case Some(tail) =>
        regex.findFirstMatchIn(tail)
          .map(regexMatch => tail.substring(regexMatch.start))
          .getOrElse("")

      case None => ""
    }
  }
  
  /**
   * Returns the index of the first comma in a string, or `string.length` if the string contains no commas.
   */
  def findFirstComma(string: String): Int = {
    string.indexOf(',') match {
      case -1 => string.length //The string does not contain any commas.
      case i => i
    }
  }

  /**
   * Trims and lowercases all user agents from a set of user agents.
   */
  def normalizeUserAgents(userAgents: Set[String]): Set[String] =
    userAgents.map(_.trim.toLowerCase(Locale.ROOT))

  /**
   * Returns a case-insensitive regular expression that matches all elements of a collection.
   * 
   * If the collection is empty, the resulting regular expression matches nothing.
   * 
   * Nothing is done to eliminate duplicates in the collection.
   */
  def regexForCollectionElements(collection: Iterable[String]): Regex = {
    if (collection.isEmpty) {
      Regex("(?!)") //This regular expression matches nothing (not even the empty string).
    } else {
      val group = collection //["foo-bar", "baz", ...]
        .map(Regex.quote) //["\Qfoo-bar\E", "\Qbaz\E", ...]
        .mkString("(?:", "|", ")") //"(?:\Qfoo-bar\E|\Qbaz\E|...)"
      
      Regex("(?i)" + group)
    }
  }

  /**
   * Removes any leading commas and whitespace characters from a string.
   */
  def removeUnnecessaryLeadingCharacters(input: String): String = {
    if (input.nonEmpty && isUnnecessaryCharacter(input.charAt(0))) {
      var index = 0

      while (index < input.length && isUnnecessaryCharacter(input.charAt(index))) {
        index += 1
      }

      input.substring(index)
    } else {
      input
    }
  }

  private def isUnnecessaryCharacter(char: Char): Boolean =
    char.isWhitespace || char == ','
}
