package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.{Directive, DirectiveParser, ParserResult, PreprocessedString}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor3}

trait DirectiveParserSpec extends AnyFlatSpec with TableDrivenPropertyChecks {
  /**
   * Common tests for key-value directive parsers.
   *
   * @param parser the [[DirectiveParser]] to test
   * @param table  A [[Table]] with three columns: ''Directive Name'', ''Directive Value'' and ''Expected Value''. The input for the parser is constructed from the first two columns.
   */
  def keyValueDirectiveParser[T](parser: DirectiveParser[T], table: TableFor3[String, String, T]): Unit = {
    it should "work with input that only contains a single directive" in {
      forEvery(table)((key, value, expectedValue) => {
        val input = s"$key: $value"
        val expected = ParserResult(Directive(key, expectedValue), "")
        val actual = parser.parse(PreprocessedString(input))
        assertResult(expected)(actual)
      })
    }

    it should "work with input that contains multiple directives" in {
      forEvery(table)((key, value, expectedValue) => {
        val suffixes = Seq(
          "foo",
          "foo, bar, baz",
          "foo: bar, baz",
          "foo, bar: baz",
          s"$key: $value" //The input ends up containing the same directive twice.
        )

        suffixes.foreach(suffix => {
          val input = s"$key: $value, $suffix"
          val expected = ParserResult(Directive(key, expectedValue), s", $suffix")
          val actual = parser.parse(PreprocessedString(input))
          assertResult(expected)(actual)
        })
      })
    }

    it should "not be affected by whitespace" in {
      forEvery(table)((key, value, expectedValue) => {
        val inputs = Seq(
          s"$key:$value", //No whitespace
          s"   $key   :   $value   " //Too much whitespace
        )

        inputs.foreach(input => {
          val expected = ParserResult(Directive(key, expectedValue), "")
          val actual = parser.parse(PreprocessedString(input))
          assertResult(expected)(actual)
        })
      })
    }

    it should "throw an exception if the value is missing" in {
      val inputs = Seq(
        "foo:",
        "foo: , bar"
      )

      inputs.foreach(input => {
        assertThrows[Exception]({
          parser.parse(PreprocessedString(input))
        })
      })
    }
  }
}
