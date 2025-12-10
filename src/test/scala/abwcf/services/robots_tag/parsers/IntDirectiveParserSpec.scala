package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.{ParserException, PreprocessedString}

class IntDirectiveParserSpec extends DirectiveParserSpec {
  private val table = Table(
    ("Directive Name", "Directive Value", "Expected Value"),
    ("max-snippet", "123", 123),
    ("max-snippet", "+123", 123),
    ("max-snippet", "-123", -123)
  )

  "IntDirectiveParser" should behave like keyValueDirectiveParser(IntDirectiveParser, table)
}
