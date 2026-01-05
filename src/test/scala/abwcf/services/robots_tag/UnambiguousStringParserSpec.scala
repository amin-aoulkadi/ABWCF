package abwcf.services.robots_tag

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks

class UnambiguousStringParserSpec extends AnyFlatSpec with TableDrivenPropertyChecks {
  private val Foo = Directive("foo")
  private val Bar = Directive("bar")
  private val Baz = Directive("baz")

  "UnambiguousStringParser" should "work with empty input" in {
    assert(UnambiguousStringParser.parse("").isEmpty)
  }

  it should "work with input that only contains a single directive" in {
    val inputs = Seq(
      "foo",
      "foo-123",
      "foo_123"
    )

    inputs.foreach(input => {
      val expected = Seq(Directive(input))
      assertResult(expected)(UnambiguousStringParser.parse(input))
    })
  }

  it should "work with input that contains multiple directives" in {
    val input = "foo, bar, baz"
    val expected = Seq(Foo, Bar, Baz)
    assertResult(expected)(UnambiguousStringParser.parse(input))
  }

  it should "trim and lowercase directive names" in {
    val input = " FOO , Bar , baz "
    val expected = Seq(Foo, Bar, Baz)
    assertResult(expected)(UnambiguousStringParser.parse(input))
  }

  it should "work with input that contains excess commas" in {
    val table = Table(
      ("Input", "Expected Result"),
      (",", Seq.empty),
      (",,,", Seq.empty),
      ("foo,", Seq(Foo)),
      (", foo", Seq(Foo)),
      ("foo, , bar,", Seq(Foo, Bar))
    )

    forEvery(table)((input, expectedResult) => {
      assertResult(expectedResult)(UnambiguousStringParser.parse(input))
    })
  }
}
