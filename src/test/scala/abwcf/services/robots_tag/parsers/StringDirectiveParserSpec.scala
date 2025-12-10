package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.{ParserException, PreprocessedString}

class StringDirectiveParserSpec extends DirectiveParserSpec {
  private val table = Table(
    ("Directive Name", "Directive Value", "Expected Value"),
    ("max-image-preview", "none", "none"),
    ("message", "Hello World!", "Hello World!") //The directive value should not be transformed to uppercase or lowercase.
  )

  "StringDirectiveParser" should behave like keyValueDirectiveParser(StringDirectiveParser, table)
}
