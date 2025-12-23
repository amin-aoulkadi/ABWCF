package abwcf.services.robots_tag

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks

import java.util.Locale

class ParserUtilsSpec extends AnyFlatSpec with TableDrivenPropertyChecks {
  "findFirstComma()" should "find the index of the first comma" in {
    val table = Table(
      ("Input", "Expected Result"),
      //Input without commas:
      ("", 0),
      ("foo", 3),
      //Input with commas:
      (",", 0),
      ("foo, bar", 3),
      ("foo, bar, baz", 3)
    )

    forEvery(table)((input, expectedResult) => {
      assertResult(expectedResult)(ParserUtils.findFirstComma(input))
    })
  }

  "normalizeUserAgents()" should "normalize user agents" in {
    val input = Set("foo", " FOO ", "Bar", " baz ")
    val expectedResult = Set("foo", "bar", "baz")
    assertResult(expectedResult)(ParserUtils.normalizeUserAgents(input))
  }

  "regexForCollectionElements()" should "create a regex that matches all collection elements" in {
    assert(ParserUtils.regexForCollectionElements(Seq.empty).findFirstMatchIn("foo bar baz").isEmpty)

    val seq = Seq("foo", "bar", "baz")
    val regex = ParserUtils.regexForCollectionElements(seq)

    assertResult(seq)(regex.findAllIn("abc foo def bar ghi baz jkl").toSeq)
    assert(regex.findFirstIn("abc def ghi jkl").isEmpty)

    seq.foreach(element => {
      assert(regex.matches(element))
      assert(regex.matches(element.toUpperCase(Locale.ROOT)))
    })
  }

  "removeUnnecessaryLeadingCharacters()" should "remove leading leading commas and whitespace characters" in {
    val table = Table(
      ("Input", "Expected Result"),
      //Input without leading clutter:
      ("", ""),
      ("foo", "foo"),
      ("foo, bar", "foo, bar"),
      ("foo, bar, ", "foo, bar, "),
      //Input with leading clutter:
      (" , ", ""),
      (", foo", "foo"),
      (", foo, bar", "foo, bar"),
      (", foo, bar, ", "foo, bar, ")
    )

    forEvery(table)((input, expectedResult) => {
      assertResult(expectedResult)(ParserUtils.removeUnnecessaryLeadingCharacters(input))
    })
  }
}
