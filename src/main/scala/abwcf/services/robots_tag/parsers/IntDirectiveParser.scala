package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.*

/**
 * Parses directives with [[Int]] values (e.g. "`max-snippet: 123`").
 */
object IntDirectiveParser extends DirectiveParser[Int] {
  override def parse(input: PreprocessedString): ParserResult[Directive[Int]] = {
    input.tail match {
      case Some(tail) =>
        val endIndex = ParserUtils.findFirstComma(tail)
        val number = Integer.parseInt(tail, 0, endIndex, 10)
        val directive = Directive(input.firstToken, number)
        val remainder = tail.substring(endIndex)

        ParserResult(directive, remainder)

      case None =>
        throw ParserException("Failed to parse key-value directive due to missing value")
    }
  }
}
