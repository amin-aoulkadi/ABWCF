package abwcf.services.robots_tag

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks

import java.time.LocalDate

class RobotsMetaParsingServiceSpec extends AnyFlatSpec with TableDrivenPropertyChecks {
  private val Follow = Directive("follow")
  private val Index = Directive("index")
  private val MaxImagePreview = Directive("max-image-preview", "large")
  private val UnavailableAfter = Directive("unavailable_after", LocalDate.of(2025, 12, 31))

  "RobotsMetaParsingService" should "initialize and reset properly" in {
    val parser = RobotsMetaParsingService()
    assert(parser.getDirectives.isEmpty)

    parser.parse("""<meta name="robots" content="index, follow">""")
    assert(parser.getDirectives.nonEmpty)

    parser.reset()
    assert(parser.getDirectives.isEmpty)
  }

  it should "work with empty inputs" in {
    val inputs = Seq(
      "",
      "<meta>",
      """<meta name="" content="index, follow">""",
      """<meta name="robots" content="">"""
    )

    val parser = RobotsMetaParsingService()
    inputs.foreach(parser.parse)
    assert(parser.getDirectives.isEmpty)
  }

  it should "parse individual directives" in {
    val parser = RobotsMetaParsingService()

    parser.parse("""<meta name="robots" content="index">""")
    assertResult(Set(Index))(parser.getDirectives)

    parser.parse("""<meta name="robots" content="max-image-preview: large">""")
    assertResult(Set(Index, MaxImagePreview))(parser.getDirectives)
  }

  it should "parse multiple directives" in {
    val parser = RobotsMetaParsingService()

    parser.parse("""<meta name="robots" content="index, follow">""")
    assertResult(Set(Index, Follow))(parser.getDirectives)

    parser.parse("""<meta name="robots" content="max-image-preview: large, unavailable_after: 2025-12-31">""")
    assertResult(Set(Index, Follow, MaxImagePreview, UnavailableAfter))(parser.getDirectives)

    parser.reset()
    parser.parse("""<meta name="robots" content="max-image-preview: large, index, unavailable_after: 2025-12-31, follow">""")
    assertResult(Set(Index, Follow, MaxImagePreview, UnavailableAfter))(parser.getDirectives)
  }

  it should "parse unknown simple directives under certain conditions" in {
    val inputs = Seq(
      //Unambiguous directive strings:
      """<meta name="robots" content="foo-123">""",
      """<meta name="robots" content="foo-123, follow">""", //First
      """<meta name="robots" content="index, foo-123, follow">""", //Middle
      """<meta name="robots" content="index, foo-123">""", //Last
      //Ambiguous directive strings:
      """<meta name="robots" content="foo-123, unavailable_after: 2025-12-31">""", //First
      """<meta name="robots" content="max-image-preview: large, foo-123, unavailable_after: 2025-12-31">""", //Middle
      """<meta name="robots" content="max-image-preview: large, foo-123">""" //Last
    )

    val expectedDirective = Directive("foo-123")

    inputs.foreach(input => {
      val parser = RobotsMetaParsingService()
      parser.parse(input)
      assert(parser.getDirectives.contains(expectedDirective))
    })
  }

  it should "never parse unknown key-value directives" in {
    val inputs = Seq(
      """<meta name="robots" content="foo: bar baz">""", //This directive string is unambiguous and could technically be parsed as a Directive[String].
      //Ambiguous directive strings:
      """<meta name="robots" content="foo: bar baz, follow">""", //First
      """<meta name="robots" content="index, foo: bar baz, follow">""", //Middle
      """<meta name="robots" content="index, foo: bar baz">""", //Last
    )

    val unexpectedDirective = Directive("foo", "bar baz")

    inputs.foreach(input => {
      val parser = RobotsMetaParsingService()
      parser.parse(input)
      assert(!parser.getDirectives.contains(unexpectedDirective))
    })
  }

  it should "trim and lowercase directive names" in {
    val parser = RobotsMetaParsingService()
    parser.parse("""<meta name="robots" content=" Index, FOLLOW ">""")
    assertResult(Set(Index, Follow))(parser.getDirectives)
  }

  it should "eliminate duplicate directives" in {
    val parser = RobotsMetaParsingService()

    //Unambiguous directive strings:
    parser.parse("""<meta name="robots" content="index, follow, index">""")
    assertResult(Set(Index, Follow))(parser.getDirectives)

    parser.parse("""<meta name="robots" content="follow">""")
    assertResult(Set(Index, Follow))(parser.getDirectives)
    
    //Ambiguous directive strings:
    parser.reset()
    parser.parse("""<meta name="robots" content="index, max-image-preview: large, index">""")
    assertResult(Set(Index, MaxImagePreview))(parser.getDirectives)

    parser.parse("""<meta name="robots" content="max-image-preview: large">""")
    assertResult(Set(Index, MaxImagePreview))(parser.getDirectives)
  }

  it should "work with input that contains excess commas" in {
    val table = Table(
      ("Input", "Expected Result"),
      //No directives:
      ("""<meta name="robots" content=",">""", Set.empty),
      ("""<meta name="robots" content=",,,">""", Set.empty),
      //Unambiguous directive strings:
      ("""<meta name="robots" content="index,">""", Set(Index)),
      ("""<meta name="robots" content=", index">""", Set(Index)),
      ("""<meta name="robots" content="index, , follow,">""", Set(Index, Follow)),
      //Ambiguous directive strings:
      ("""<meta name="robots" content="max-image-preview: large,">""", Set(MaxImagePreview)),
      ("""<meta name="robots" content=", max-image-preview: large">""", Set(MaxImagePreview)),
      ("""<meta name="robots" content="max-image-preview: large, , unavailable_after: 2025-12-31,">""", Set(MaxImagePreview, UnavailableAfter))
    )

    forEvery(table)((input, expectedResult) => {
      val parser = RobotsMetaParsingService()
      parser.parse(input)
      assertResult(expectedResult)(parser.getDirectives)
    })
  }

  it should "work with different HTML attribute syntaxes" in {
    val inputs = Seq(
      //Double-quoted attribute values:
      """<meta name="robots" content="index, follow">""",
      """<meta name = "robots" content="index, follow">""",
      //Single-quoted attribute values:
      """<meta name='robots' content='index, follow'>""",
      """<meta name = 'robots' content='index, follow'>""",
      //Unquoted attribute values:
      """<meta name=robots content=index,follow>""",
      """<meta name = robots content = index,follow>"""
    )

    inputs.foreach(input => {
      val parser = RobotsMetaParsingService()
      parser.parse(input)
      assertResult(Set(Index, Follow))(parser.getDirectives)
    })
  }

  it should "work with unusual HTML attribute orders" in {
    val parser = RobotsMetaParsingService()
    parser.parse("""<meta content="index, follow" name="robots">""")
    assertResult(Set(Index, Follow))(parser.getDirectives)
  }

  it should "parse HTML in a case-insensitive manner" in {
    val parser = RobotsMetaParsingService()
    parser.parse("""<META Name="robots" Content="index">""")
    assertResult(Set(Index))(parser.getDirectives)
  }

  it should "ignore unrelated HTML attributes" in {
    val parser = RobotsMetaParsingService()

    parser.parse("""<meta some-name="robots" some-content="index, follow">""") //"name" and "content" are just suffixes of "some-name" and "some-content", so the parser should ignore them.
    parser.parse("""<meta foo="bar" baz>""")
    assert(parser.getDirectives.isEmpty)

    parser.parse("""<meta foo="bar" name="robots" baz content="index, follow">""")
    assertResult(Set(Index, Follow), parser.getDirectives)
  }

  it should "handle exceptions as configured" in {
    val inputs = Seq(
      """<meta name="robots" content="foo: bar, index">""", //There is no suitable DirectiveParser for "foo" key-value directives.
      """<meta name="robots" content="max-snippet: baz, FOLLOW">""" //There is a suitable DirectiveParser for "max-snippet" key-value directives, but "baz" is an invalid value. The "follow" directive is parsable, but not normalized.
    )

    //Throw exceptions:
    val throwingParser = RobotsMetaParsingService(exceptionHandler = ExceptionHandlers.throwing)

    inputs.foreach(input => {
      assertThrows[ParserException](throwingParser.parse(input))
    })

    assert(throwingParser.getDirectives.isEmpty) //The "index" and "follow" directives were not parsed because the exception handler threw the exceptions.

    //Ignore exceptions:
    val ignoringParser = RobotsMetaParsingService(exceptionHandler = ExceptionHandlers.ignoring)
    inputs.foreach(ignoringParser.parse)
    assertResult(Set(Index, Follow))(ignoringParser.getDirectives) //The "index" and "follow" directives were parsed because the exception handler ignored the exceptions.
  }

  "RobotsMetaParsingService (without target user agents)" should "collect directives that apply to all user agents" in {
    val parser = RobotsMetaParsingService()
    parser.parse("""<meta name="robots" content="index">""")
    assertResult(Set(Index))(parser.getDirectives)
  }

  it should "not collect directives that only apply to specific user agents" in {
    val parser = RobotsMetaParsingService()
    parser.parse("""<meta name="UnknownBot" content="all">""")
    assert(parser.getDirectives.isEmpty)
  }

  "RobotsMetaParsingService (with target user agents)" should "collect directives that apply to all user agents" in {
    val parser = RobotsMetaParsingService(Set("MyBot-1", "MyBot-2"))
    parser.parse("""<meta name="robots" content="index">""")
    assertResult(Set(Index))(parser.getDirectives)
  }

  it should "collect directives that apply to the target user agents" in {
    val parser = RobotsMetaParsingService(Set("MyBot-1", "MyBot-2"))

    parser.parse("""<meta name="MyBot-1" content="index">""")
    assertResult(Set(Index))(parser.getDirectives)

    parser.parse("""<meta name="MyBot-2" content="follow">""")
    assertResult(Set(Index, Follow))(parser.getDirectives)
  }

  it should "not collect directives that only apply to other user agents" in {
    val parser = RobotsMetaParsingService(Set("MyBot"))
    parser.parse("""<meta name="UnknownBot" content="index">""")
    assert(parser.getDirectives.isEmpty)
  }

  it should "perform case-insensitive user agent matching" in {
    val parser = RobotsMetaParsingService(Set("MyBot"))
    parser.parse("""<meta name="mybot" content="index">""")
    assertResult(Set(Index))(parser.getDirectives)
  }
}
