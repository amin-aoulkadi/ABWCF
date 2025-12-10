package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.{Directive, DirectiveParser, ParserResult, PreprocessedString}

/**
 * Parses simple directives that have no value (e.g. "`follow`" or "`index`").
 */
object SimpleDirectiveParser extends DirectiveParser[Nothing] {
  override def parse(input: PreprocessedString): ParserResult[Directive[Nothing]] = {
    val directive = Directive(input.firstToken, None)
    val remainder = input.string.substring(input.delimiterIndex)
    ParserResult(directive, remainder)
  }
}
