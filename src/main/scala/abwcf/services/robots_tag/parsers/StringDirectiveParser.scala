package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.*

/**
 * Parses directives with [[String]] values (e.g. "`max-image-preview: none`").
 *
 * Everything up to the first comma is considered to be part of the directive value.
 *
 * The directive value is trimmed, but not modified otherwise (i.e. it is not transformed to uppercase or lowercase).
 */
object StringDirectiveParser extends DirectiveParser[String] {
  override def parse(input: PreprocessedString): ParserResult[Directive[String]] = {
    input.tail match {
      case Some(tail) =>
        val endIndex = ParserUtils.findFirstComma(tail)
        val string = tail.substring(0, endIndex).trim

        if (string.isEmpty) {
          throw ParserException("Failed to parse key-value directive due to missing value")
        }

        val directive = Directive(input.firstToken, string)
        val remainder = tail.substring(endIndex)

        ParserResult(directive, remainder)

      case None =>
        throw ParserException("Failed to parse key-value directive due to missing value")
    }
  }
}
