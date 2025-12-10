package abwcf.services.robots_tag

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks

class PreprocessedStringSpec extends AnyFlatSpec with TableDrivenPropertyChecks {
  def test(input: String, expected: PreprocessedString): Unit = {
    val actual = PreprocessedString(input)
    assertResult(expected)(actual)
  }

  "PreprocessedString" should "work with input that only contains a single token" in {
    val input = "token"
    val expected = PreprocessedString(input, "token", 5, None, None)
    test(input, expected)
  }

  it should "work with input that contains multiple comma-separated tokens (but no colons)" in {
    val input = "token-1, token-2, token-3"
    val expected = PreprocessedString(input, "token-1", 7, ',', "token-2, token-3")
    test(input, expected)
  }

  it should "work with input that contains two colon-separated tokens (but no commas)" in {
    val input = "key: value"
    val expected = PreprocessedString(input, "key", 3, ':', "value")
    test(input, expected)
  }

  it should "work with input that contains both comma- and colon-separated tokens (comma first)" in {
    val input = "first-token, second-token: third-token"
    val expected = PreprocessedString(input, "first-token", 11, ',', "second-token: third-token")
    test(input, expected)
  }

  it should "work with input that contains both comma- and colon-separated tokens (colon first)" in {
    val input = "first-token: second-token, third-token"
    val expected = PreprocessedString(input, "first-token", 11, ':', "second-token, third-token")
    test(input, expected)
  }

  it should "trim and lowercase the first token" in {
    val input = " TOKEN_1 , TOKEN_2 "
    val expected = PreprocessedString(input, "token_1", 9, ',', "TOKEN_2")
    test(input, expected)
  }

  it should "trim the tail" in {
    val input = " message : Hello World! "
    val expected = PreprocessedString(input, "message", 9, ':', "Hello World!")
    test(input, expected)
  }

  it should "not be affected by the absence of whitespace" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("a,b,c", PreprocessedString("a,b,c", "a", 1, ',', "b,c")),
      ("a:b:c", PreprocessedString("a:b:c", "a", 1, ':', "b:c"))
    )

    forEvery(table)(test)
  }

  it should "handle orphan delimiters" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("token,", PreprocessedString("token,", "token", 5, Some(','), None)),
      ("token:", PreprocessedString("token:", "token", 5, Some(':'), None)),
      ("a: , b, c", PreprocessedString("a: , b, c", "a", 1, ':', ", b, c"))
    )

    forEvery(table)(test)
  }
}
