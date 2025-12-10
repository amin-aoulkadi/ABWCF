package abwcf.services.robots_tag

import java.util.Locale

/**
 * Parses unambiguous directive strings.
 *
 * A directive string is unambiguous if it does not contain any colons. As such, unambiguous strings can neither contain any key-value directives, nor can they contain directives that only apply to specific user agents.
 * 
 * An unambiguous string can be treated as a string of comma-separated directives.
 */
object UnambiguousStringParser {
  /**
   * Parses unambiguous directive strings. Nothing is done to ensure that the input is unambiguous.
   *
   * The result may contain duplicates.
   */
  def parse(input: String): Iterable[Directive[Nothing]] = {
    input.toLowerCase(Locale.ROOT) //"foo, bar, "
      .split(',') //["foo", " bar", " "]
      .map(_.trim) //["foo", "bar", ""]
      .filter(_.nonEmpty) //["foo", "bar"]
      .map(Directive(_, None))
  }
}
