package abwcf.services.robots_tag

import abwcf.services.robots_tag.parsers.SimpleDirectiveParser

/**
 * Parses the content of `X-Robots-Tag` HTTP response headers.
 *
 * This parser is not thread-safe.
 *
 * @param targetUserAgents       The target user agents.
 *                               The parser collects directives if they apply to a target user agent.
 *                               Directives that only apply to non-target user agents are not collected.
 *                               Directives that apply to all user agents are always collected.
 * @param directiveParsersByName Trimmed and lowercased directive names mapped to [[DirectiveParser]]s that can parse the corresponding directives.
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
                              directiveParsersByName: Map[String, DirectiveParser[?]] = KnownDirectiveParsers.DefaultParsersByName,
                              exceptionHandler: ParserException => Unit = ExceptionHandlers.ignoring) {
  /**
   * The target user agents configured by the user, normalized.
   */
  private val normalizedTargetUserAgents = targetUserAgents.map(ParserUtils.normalizeUserAgent)

  /**
   * A regular expression that matches all directive names known to the parser.
   */
  private val knownDirectiveNamesRegex = ParserUtils.regexForCollectionElements(directiveParsersByName.keys)

  /**
   * A regular expression that matches all user agents known to the parser.
   */
  private val knownUserAgentsRegex = ParserUtils.regexForCollectionElements(normalizedTargetUserAgents)

  /**
   * All directives that have been collected since the last reset.
   */
  private val directiveCollection = ModifiableDirectiveCollection()

  /**
   * Parses an `X-Robots-Tag` HTTP response header.
   *
   * This method can handle empty strings.
   *
   * Nothing is done to ensure that the input is an `X-Robots-Tag` header.
   *
   * @param robotsHeader a single `X-Robots-Tag` header (without the "X-Robots-Tag:" prefix)
   * @throws Exception if the [[exceptionHandler]] throws an exception
   */
  def parse(robotsHeader: String): Unit = {
    if (robotsHeader.contains(':')) {
      parseAmbiguousString(robotsHeader)
    } else {
      UnambiguousStringParser.parse(robotsHeader).foreach(directiveCollection.addDirective)
    }
  }

  /**
   * Parses an ambiguous directive string.
   *
   * A directive string is ambiguous if it contains at least one colon. In `X-Robots-Tag` headers, a colon indicates a user agent group or a key-value directive. There are three problems:
   *  - Colons that separate user agents from directives are indistinguishable from colons that separate directive names from directive values.
   *  - Some directive values contain unescaped commas, which are indistinguishable from commas that separate directives.
   *  - Some directive values contain unescaped colons, which are indistinguishable from colons that separate directive names from directive values.
   *
   * An ambiguous string can not be treated as a string of comma-separated directives. Instead, it has to be parsed token by token.
   *
   * @throws Exception if the [[exceptionHandler]] throws an exception
   */
  private def parseAmbiguousString(robotsHeader: String): Unit = {
    var stringToParse = ParserUtils.removeUnnecessaryLeadingCharacters(robotsHeader)
    var currentUserAgent: Option[String] = None

    while (stringToParse.nonEmpty) {
      val preprocessed = PreprocessedString(stringToParse)

      if (preprocessed.delimiter.contains(':') && normalizedTargetUserAgents.contains(preprocessed.firstToken)) { //The first token is a target user agent.
        currentUserAgent = Some(preprocessed.firstToken)

        //Remove the user agent from the string:
        stringToParse = preprocessed.tail
          .map(ParserUtils.removeUnnecessaryLeadingCharacters)
          .getOrElse("")
      } else { //The first token is either a directive name or an unknown user agent.
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
              //Parse and collect the first directive:
              val parserResult = parser.parse(preprocessed)

              currentUserAgent match {
                case Some(userAgent) => directiveCollection.addDirective(userAgent, parserResult.value)
                case None => directiveCollection.addDirective(parserResult.value)
              }

              //Remove the parsed directive (including its value, if applicable) from the string:
              stringToParse = ParserUtils.removeUnnecessaryLeadingCharacters(parserResult.remainder)
            } catch {
              case e: Exception => //The first token is a directive name.
                exceptionHandler.apply(ParserException(s"Failed to parse the first directive in \"$stringToParse\"", e))

                stringToParse = preprocessed.tail match {
                  case Some(tail) =>
                    val colonIndex = tail.indexOf(':')

                    knownDirectiveNamesRegex.findFirstMatchIn(tail) match {
                      case Some(regexMatch) if regexMatch.start < colonIndex || colonIndex == -1 =>
                        tail.substring(regexMatch.start) //Skipping to the next known directive name is safe as long as no colons are skipped over.

                      case _ => //Either there is no next known directive name, or the next known directive name comes after a colon. That colon could indicate a user agent group, so ...
                        ParserUtils.dropUntilFirstMatch(knownUserAgentsRegex, preprocessed) //... skipping to the next known user agent is the only safe option.
                    }

                  case None => ""
                }
            }

          case None => //The first token is either an unknown key-value directive name or an unknown user agent.
            exceptionHandler.apply(ParserException(s"Failed to parse unknown token \"${preprocessed.firstToken}\". Is it a directive name or a user agent?"))
            stringToParse = ParserUtils.dropUntilFirstMatch(knownUserAgentsRegex, preprocessed) //The first token could be an unknown user agent, so skipping to the next known directive name risks collecting directives that only apply to an unknown user agent. As such, skipping to the next known user agent is the only safe option.
        }
      }
    }
  }

  /**
   * Returns all directives that have been collected since the last reset.
   */
  def collectedDirectives: DirectiveCollection =
    directiveCollection

  /**
   * Clears the set of collected directives.
   */
  def reset(): Unit =
    directiveCollection.clear()
}
