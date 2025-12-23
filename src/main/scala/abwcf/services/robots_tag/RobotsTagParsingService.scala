package abwcf.services.robots_tag

import abwcf.services.robots_tag.parsers.SimpleDirectiveParser

import scala.collection.mutable

/**
 * Parses the content of `X-Robots-Tag` HTTP response headers.
 *
 * This parser is not thread-safe.
 *
 * @param targetUserAgents       The target user agents.
 *                               The parser collects directives that apply to a target user agent.
 *                               Directives that only apply to non-target user agents are not collected.
 *                               Directives that apply to all user agents are always collected.
 * @param knownUserAgents        The set of known user agents.
 *                               This set may include both target and non-target user agents.
 *                               The default value is [[KnownUserAgents.DefaultUserAgents]].
 * @param directiveParsersByName Lowercased and trimmed directive names mapped to [[DirectiveParser]]s that can parse the corresponding directives.
 *                               The default value is [[KnownDirectiveParsers.DefaultParsersByName]].
 * @param exceptionHandler       The parser invokes this function when it encounters a [[ParserException]] while parsing.
 *                               Use this function to ignore, throw, log, count or collect exceptions.
 *                               If this function throws the encountered exception, the parser stops parsing the current input.
 *                               If the exception is not thrown, the parser advances to the next known directive or user agent and continues to parse the rest of the current input.
 *                               The default value is [[ExceptionHandlers.ignoring]].
 * @note Creating a new [[RobotsTagParsingService]] instance incurs some overhead (normalizing user agents, compiling regular expressions).
 *       It is therefore recommended to reuse existing parser instances (with [[reset]]) if possible.
 * @note There is no official standard or specification for `X-Robots-Tag` headers.
 *       In some cases, their syntax is ambiguous, which makes parsing difficult.
 *       Different vendors may define and support different directives.
 * @see
 *      - [[https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/X-Robots-Tag MDN: X-Robots-Tag]]
 *      - [[https://developers.google.com/search/docs/crawling-indexing/robots-meta-tag Google: Robots Meta Tags Specifications]]
 */
class RobotsTagParsingService(targetUserAgents: Set[String] = Set.empty,
                              knownUserAgents: Set[String] = KnownUserAgents.DefaultUserAgents,
                              directiveParsersByName: Map[String, DirectiveParser[?]] = KnownDirectiveParsers.DefaultParsersByName,
                              exceptionHandler: ParserException => Unit = ExceptionHandlers.ignoring) {
  /**
   * The target user agents configured by the user, normalized.
   */
  private val normalizedTargetUserAgents = ParserUtils.normalizeUserAgents(targetUserAgents)

  /**
   * The known user agents, normalized.
   */
  private val normalizedKnownUserAgents = ParserUtils.normalizeUserAgents(knownUserAgents)

  /**
   * All directives collected by the parser so far.
   */
  private val parsedDirectives = mutable.Set.empty[Directive[?]]

  /**
   * A regular expression that matches all directive names and user agents known to the parser.
   */
  private val knownDirectiveNamesAndUserAgentsRegex = ParserUtils.regexForCollectionElements(directiveParsersByName.keys.concat(normalizedTargetUserAgents).concat(normalizedKnownUserAgents))

  /**
   * A regular expression that matches all user agents known to the parser.
   */
  private val knownUserAgentsRegex = ParserUtils.regexForCollectionElements(normalizedTargetUserAgents.concat(normalizedKnownUserAgents))

  def parse(robotsHeader: String): Unit = {
    if (robotsHeader.contains(':')) {
      parseAmbiguousString(robotsHeader)
    } else {
      parsedDirectives.addAll(UnambiguousStringParser.parse(robotsHeader))
    }
  }

  private def parseAmbiguousString(robotsHeader: String): Unit = {
    var stringToParse = ParserUtils.removeUnnecessaryLeadingCharacters(robotsHeader)
    var shouldCollect = true

    while (stringToParse.nonEmpty) {
      val preprocessed = PreprocessedString(stringToParse)
      var isUserAgent = false

      //Check if the first token is a user agent:
      if (preprocessed.delimiter.contains(':')) { //If the first delimiter is a colon, then the first token might be a user agent.
        if (normalizedTargetUserAgents.contains(preprocessed.firstToken)) { //The first token is a target user agent.
          shouldCollect = true
          isUserAgent = true
        } else if (normalizedKnownUserAgents.contains(preprocessed.firstToken)) { //The first token is a known non-target user agent.
          shouldCollect = false
          isUserAgent = true
        }
      }

      if (isUserAgent) {
        //Remove the user agent from the string:
        stringToParse = preprocessed.tail
          .map(ParserUtils.removeUnnecessaryLeadingCharacters)
          .getOrElse("")
      } else {
        //Find a suitable DirectiveParser:
        val parserOption =
          if (preprocessed.delimiter.contains(':')) { //If the first delimiter is a colon, then the first token is either a key-value directive or an unknown user agent.
            directiveParsersByName.get(preprocessed.firstToken)
          } else { //If the first delimiter is a comma or None, then the directive must be a simple directive without a value.
            Some(SimpleDirectiveParser)
          }

        parserOption match {
          case Some(parser) =>
            try {
              //Parse the first directive:
              val parserResult = parser.parse(preprocessed)

              if (shouldCollect) { //Only collect directives if they apply to a target user agent or to all user agents.
                parsedDirectives.add(parserResult.value)
              }

              //Remove the parsed directive (including its value, if applicable) from the string:
              stringToParse = ParserUtils.removeUnnecessaryLeadingCharacters(parserResult.remainder)
            } catch {
              case e: Exception =>
                exceptionHandler.apply(ParserException(s"Failed to parse the first directive in \"$stringToParse\"", e))
                stringToParse = dropUntilNextKnownToken(preprocessed) //The first token is definitely a directive.
            }

          case None =>
            exceptionHandler.apply(ParserException(s"Failed to parse unknown token \"${preprocessed.firstToken}\". Is it a directive name or a user agent?"))
            stringToParse = dropUntilNextKnownUserAgent(preprocessed) //The first token is either an unknown directive name or an unknown user agent.
        }
      }
    }
  }

  /**
   * Removes the first token and everything up to the next known directive name or user agent from a string.
   */
  private def dropUntilNextKnownToken(preprocessed: PreprocessedString): String = {
    preprocessed.tail match {
      case Some(tail) =>
        knownDirectiveNamesAndUserAgentsRegex.findFirstMatchIn(tail)
          .map(regexMatch => tail.substring(regexMatch.start))
          .getOrElse("")

      case None => ""
    }
  }

  /**
   * Removes the first token and everything up to the next known user agent from a string.
   */
  private def dropUntilNextKnownUserAgent(preprocessed: PreprocessedString): String = {
    preprocessed.tail match {
      case Some(tail) =>
        knownUserAgentsRegex.findFirstMatchIn(tail)
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
