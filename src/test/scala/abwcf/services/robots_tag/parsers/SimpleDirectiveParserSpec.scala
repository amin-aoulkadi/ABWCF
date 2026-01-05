package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.{Directive, ParserResult, PreprocessedString}

class SimpleDirectiveParserSpec extends DirectiveParserSpec {
  "SimpleDirectiveParser" should "work with input that only contains a single directive" in {
    val inputs = Seq(
      "foo",
      "foo-123",
      "foo_123"
    )

    inputs.foreach(input => {
      val expected = ParserResult(Directive(input, None), "")
      val actual = SimpleDirectiveParser.parse(PreprocessedString(input))
      assertResult(expected)(actual)
    })
  }

  it should "work with input that contains multiple directives" in {
    val directiveName = "all"

    val suffixes = Seq(
      "foo",
      "foo, bar, baz",
      "foo: bar, baz",
      "foo, bar: baz",
      directiveName //The input ends up containing the same directive twice.
    )

    suffixes.foreach(suffix => {
      val input = s"$directiveName, $suffix"
      val expected = ParserResult(Directive(directiveName, None), s", $suffix")
      val actual = SimpleDirectiveParser.parse(PreprocessedString(input))
      assertResult(expected)(actual)
    })
  }
}
