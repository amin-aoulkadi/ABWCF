package abwcf.services.robots_tag

import abwcf.services.robots_tag.parsers.SimpleDirectiveParser

import java.util.Locale
import scala.collection.mutable
import scala.util.matching.Regex

object RobotsMetaParsingService {
  /*
  The regular expressions below are designed to retrieve the values of specific attributes from HTML elements.
  The only difference between the regular expressions is the attribute name.

  Things to consider:
    - There is always at least one whitespace character before the attribute name.
      The regular expressions must check for this whitespace character to avoid matching suffixes of a longer attribute names.
    - Attribute names are case-insensitive.
    - There may be whitespace characters around the equals sign that separates the attribute name from the attribute value.
    - Attribute values can be double-quoted, single-quoted or unquoted.

  Reference: https://html.spec.whatwg.org/multipage/syntax.html (→ 13.1.2.3 Attributes)
   */

  private val NameRegex = Regex("""(?i)\sname\s*=\s*("[^"]+"|'[^']+'|[^\s"'=<>`]+)""")
  private val ContentRegex = Regex("""(?i)\scontent\s*=\s*("[^"]+"|'[^']+'|[^\s"'=<>`]+)""")

  /**
   * Extracts the value of an attribute from an HTML element.
   *
   * @param attributeRegex A regular expression that matches the desired attribute. The first capturing group must contain the attribute value.
   * @param htmlElement the HTML element
   * @return the value of the attribute, or [[None]] if the desired attribute is not present
   */
  private def getAttributeValue(attributeRegex: Regex, htmlElement: String): Option[String] = {
    attributeRegex.findFirstMatchIn(htmlElement)
      .map(regexMatch => {
        val value = regexMatch.group(1) //The group at index 0 contains the entire match. The first capturing group is at index 1.
        val firstChar = value.charAt(0)

        if (firstChar == '"' || firstChar == '\'') {
          value.substring(1, value.length - 1) //"'foo'" → "foo"
        } else {
          value
        }
      })
  }
}

/**
 * Parses `<meta name="robots" content="...">` HTML elements.
 *
 * This parser is not thread-safe.
 *
 * @param targetUserAgents       The target user agents.
 *                               The parser collects directives that apply to a target user agent.
 *                               Directives that only apply to non-target user agents are not collected.
 *                               Directives that apply to all user agents are always collected.
 * @param directiveParsersByName Lowercased and trimmed directive names mapped to [[DirectiveParser]]s that can parse the corresponding directives.
 *                               The default value is [[KnownDirectiveParsers.DefaultParsersByName]].
 * @param exceptionHandler       The parser invokes this function when it encounters a [[ParserException]] while parsing.
 *                               Use this function to ignore, throw, log, count or collect exceptions.
 *                               If this function throws the encountered exception, the parser stops parsing the current input.
 *                               If the exception is not thrown, the parser advances to the next known directive and continues to parse the rest of the current input.
 *                               The default value is [[ExceptionHandlers.ignoring]].
 * @note Creating a new [[RobotsMetaParsingService]] instance incurs some overhead (normalizing user agents, compiling regular expressions).
 *       It is therefore recommended to reuse existing parser instances (with [[reset]]) if possible.
 * @note There is no official standard or specification for `<meta name="robots">` elements.
 *       In some cases, their syntax is ambiguous, which makes parsing difficult.
 *       Different vendors may define and support different directives.
 * @see
 *      - [[https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/meta/name/robots MDN: &lt;meta name=&quot;robots&quot;&gt;]]
 *      - [[https://developers.google.com/search/docs/crawling-indexing/robots-meta-tag Google: Robots Meta Tags Specifications]]
 */
class RobotsMetaParsingService(targetUserAgents: Set[String] = Set.empty,
                               directiveParsersByName: Map[String, DirectiveParser[?]] = KnownDirectiveParsers.DefaultParsersByName,
                               exceptionHandler: ParserException => Unit = ExceptionHandlers.ignoring) {
  import RobotsMetaParsingService.*

  /**
   * The target user agents configured by the user, normalized.
   */
  private val normalizedTargetUserAgents = ParserUtils.normalizeUserAgents(targetUserAgents)

  /**
   * All directives collected by the parser so far.
   */
  private val parsedDirectives = mutable.Set.empty[Directive[?]]

  /**
   * A regular expression that matches all directive names known to the parser.
   */
  private val knownDirectiveNamesRegex = ParserUtils.regexForCollectionElements(directiveParsersByName.keys)

  /**
   * Parses a `<meta name="robots" content="...">` HTML element.
   *
   * If the element has a `name` and a `content` attribute and if the value of the `name` attribute is equal to "robots" or one of the configured user agents, then the directives from the `content` attribute are parsed and collected.
   *
   * This method can handle empty strings, empty `<meta>` elements, `<meta>` elements that lack the required attributes, and `<meta>` elements that have unrelated attributes.
   *
   * Nothing is done to ensure that the input is a `<meta>` element.
   *
   * @param metaElement a single `<meta>` element
   * @throws Exception if the [[exceptionHandler]] throws an exception
   */
  def parse(metaElement: String): Unit = {
    val nameOption = getAttributeValue(NameRegex, metaElement)
    val contentOption = getAttributeValue(ContentRegex, metaElement)

    (nameOption, contentOption) match {
      case (Some(name), Some(content)) =>
        val normalizedName = name.trim.toLowerCase(Locale.ROOT)

        if (normalizedName == "robots" || normalizedTargetUserAgents.contains(normalizedName)) {
          if (content.contains(':')) {
            parseAmbiguousString(content)
          } else {
            parsedDirectives.addAll(UnambiguousStringParser.parse(content))
          }
        }

      case _ => ()
    }
  }

  /**
   * Parses an ambiguous directive string.
   *
   * A directive string is ambiguous if it contains at least one colon. In `<meta name="robots">` elements, a colon indicates a key-value directive. But there are two problems:
   *  - Some directive values contain unescaped commas, which are indistinguishable from commas that separate directives.
   *  - Some directive values contain unescaped colons, which are indistinguishable from colons that separate keys from values.
   *
   * An ambiguous string can therefore not be treated as a string of comma-separated directives. Instead, it has to be parsed directive by directive.
   *
   * @throws Exception if the [[exceptionHandler]] throws an exception
   */
  private def parseAmbiguousString(content: String): Unit = {
    var stringToParse = ParserUtils.removeUnnecessaryLeadingCharacters(content)

    while (stringToParse.nonEmpty) {
      val preprocessed = PreprocessedString(stringToParse)

      //Find a suitable DirectiveParser:
      val parserOption =
        if (preprocessed.delimiter.contains(':')) { //If the first delimiter is a colon, then the directive must be a key-value directive.
          directiveParsersByName.get(preprocessed.firstToken)
        } else { //If the first delimiter is a comma or None, then the directive must be a simple directive without a value.
          Some(SimpleDirectiveParser)
        }

      parserOption match {
        case Some(parser) =>
          try {
            //Parse the first directive:
            val parserResult = parser.parse(preprocessed)
            parsedDirectives.add(parserResult.value)

            //Remove the parsed directive (including its value, if applicable) from the string:
            stringToParse = ParserUtils.removeUnnecessaryLeadingCharacters(parserResult.remainder)
          } catch {
            case e: Exception =>
              exceptionHandler.apply(ParserException(s"Failed to parse the first directive in \"$stringToParse\"", e))
              stringToParse = dropUntilNextKnownDirective(preprocessed)
          }

        case None =>
          exceptionHandler.apply(ParserException(s"Failed to find a suitable DirectiveParser for \"${preprocessed.firstToken}\""))
          stringToParse = dropUntilNextKnownDirective(preprocessed)
      }
    }
  }

  /**
   * Removes the first token and everything up to the next known directive from a string.
   *
   * When parsing a directive from an ambiguous directive string fails with an exception, the parser does not know where the problematic directive ends. This method allows the parser to find the next token (after the problematic directive) that is definitely a directive, so that the rest of the string can be parsed.
   */
  private def dropUntilNextKnownDirective(preprocessed: PreprocessedString): String = {
    preprocessed.tail match {
      case Some(tail) =>
        knownDirectiveNamesRegex.findFirstMatchIn(tail)
          .map(regexMatch => tail.substring(regexMatch.start))
          .getOrElse("")

      case None => ""
    }
  }

  def getDirectives: Set[Directive[?]] =
    Set.from(parsedDirectives) //Creates an immutable copy.

  def reset(): Unit =
    parsedDirectives.clear()
}
