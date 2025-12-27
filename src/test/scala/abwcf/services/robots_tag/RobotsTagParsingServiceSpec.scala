package abwcf.services.robots_tag

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks

import java.time.{LocalDate, ZoneOffset, ZonedDateTime}

class RobotsTagParsingServiceSpec extends AnyFlatSpec with TableDrivenPropertyChecks {
  private val Follow = Directive("follow")
  private val Index = Directive("index")
  private val MaxImagePreview = Directive("max-image-preview", "large")
  private val UnavailableAfter = Directive("unavailable_after", LocalDate.of(2025, 12, 31))

  "RobotsTagParsingService" should "initialize and reset properly" in {
    val parser = RobotsTagParsingService()
    assert(parser.getDirectives.isEmpty)

    parser.parse("index, follow")
    assert(parser.getDirectives.nonEmpty)

    parser.reset()
    assert(parser.getDirectives.isEmpty)
  }

  it should "work with empty input" in {
    val parser = RobotsTagParsingService()
    parser.parse("")
    assert(parser.getDirectives.isEmpty)
  }

  it should "parse individual directives" in {
    val parser = RobotsTagParsingService()

    parser.parse("index")
    assertResult(Set(Index))(parser.getDirectives)

    parser.parse("max-image-preview: large")
    assertResult(Set(Index, MaxImagePreview))(parser.getDirectives)
  }

  it should "parse multiple directives" in {
    val parser = RobotsTagParsingService()

    parser.parse("index, follow")
    assertResult(Set(Index, Follow))(parser.getDirectives)

    parser.parse("max-image-preview: large, unavailable_after: 2025-12-31")
    assertResult(Set(Index, Follow, MaxImagePreview, UnavailableAfter))(parser.getDirectives)

    parser.reset()
    parser.parse("max-image-preview: large, index, unavailable_after: 2025-12-31, follow")
    assertResult(Set(Index, Follow, MaxImagePreview, UnavailableAfter))(parser.getDirectives)
  }

  it should "parse unknown simple directives under certain conditions" in {
    val inputs = Seq(
      //Unambiguous directive strings:
      "foo-123",
      "foo-123, follow", //First
      "index, foo-123, follow", //Middle
      "index, foo-123", //Last
      //Ambiguous directive strings:
      "foo-123, unavailable_after: 2025-12-31", //First
      "max-image-preview: large, foo-123, unavailable_after: 2025-12-31", //Middle
      "max-image-preview: large, foo-123" //Last
    )

    val expectedDirective = Directive("foo-123")

    inputs.foreach(input => {
      val parser = RobotsTagParsingService()
      parser.parse(input)
      assert(parser.getDirectives.contains(expectedDirective))
    })
  }

  it should "trim and lowercase directive names" in {
    val parser = RobotsTagParsingService()
    parser.parse(" Index, FOLLOW ") //Unambiguous directive string
    parser.parse(" Max-Image-Preview : large ") //Ambiguous directive string
    assertResult(Set(Index, Follow, MaxImagePreview))(parser.getDirectives)
  }

  it should "eliminate duplicate directives" in {
    val parser = RobotsTagParsingService()

    //Unambiguous directive strings:
    parser.parse("index, follow, index")
    assertResult(Set(Index, Follow))(parser.getDirectives)

    parser.parse("follow")
    assertResult(Set(Index, Follow))(parser.getDirectives)

    //Ambiguous directive strings:
    parser.reset()
    parser.parse("index, max-image-preview: large, index")
    assertResult(Set(Index, MaxImagePreview))(parser.getDirectives)

    parser.parse("max-image-preview: large")
    assertResult(Set(Index, MaxImagePreview))(parser.getDirectives)
  }

  it should "work with input that contains excess commas" in {
    val table = Table(
      ("Input", "Expected Result"),
      //No directives:
      (",", Set.empty),
      (",,,", Set.empty),
      //Unambiguous directive strings:
      ("index,", Set(Index)),
      (", index", Set(Index)),
      ("index, , follow,", Set(Index, Follow)),
      //Ambiguous directive strings:
      ("max-image-preview: large,", Set(MaxImagePreview)),
      (", max-image-preview: large", Set(MaxImagePreview)),
      ("max-image-preview: large, , unavailable_after: 2025-12-31,", Set(MaxImagePreview, UnavailableAfter))
    )

    forEvery(table)((input, expectedResult) => {
      val parser = RobotsTagParsingService()
      parser.parse(input)
      assertResult(expectedResult)(parser.getDirectives)
    })
  }

  it should "throw exceptions if configured to do so" in {
    val inputs = Seq(
      "foo: bar, index", //It is unclear whether "foo" is a directive name or a user agent.
      "max-snippet: <invalid value>, follow"
    )

    val parser = RobotsTagParsingService(exceptionHandler = ExceptionHandlers.throwing)

    inputs.foreach(input => {
      assertThrows[ParserException](parser.parse(input))
    })

    assert(parser.getDirectives.isEmpty) //The "index" and "follow" directives were not parsed because the exception handler threw the exceptions.
  }

  it should "recover from parsing failures" in {
    val table = Table(
      ("Input", "Expected Result"),
      //The first token is part of an unknown key-value pair (skip to the next known user agent):
      ("foo: bar, index", Set.empty), //It is unclear whether "foo" is a directive name or a user agent.
      ("foo: bar, max-image-preview: large", Set.empty),
      ("foo: bar, MyBot: index", Set(Index)),
      ("foo: bar, MyBot: max-image-preview: large", Set(MaxImagePreview)),
      //A DirectiveParser throws (skip to the next known directive name):
      ("max-snippet: <invalid value>, index", Set(Index)),
      ("max-snippet: <invalid value>, max-image-preview: large", Set(MaxImagePreview)),
      ("max-snippet: <invalid value>, index, UnknownBot: max-image-preview: large", Set(Index)),
      ("max-snippet: <invalid value>, max-image-preview: large, UnknownBot: index", Set(MaxImagePreview)),
      //A DirectiveParser throws (skip to the next known user agent):
      ("max-snippet: <invalid value>, UnknownBot: follow", Set.empty),
      ("max-snippet: <invalid value>, UnknownBot: max-image-preview: large", Set.empty),
      ("max-snippet: <invalid value>, UnknownBot: follow, MyBot: index", Set(Index)),
      ("max-snippet: <invalid value>, UnknownBot: max-image-preview: large, MyBot: index", Set(Index)),
      ("unavailable_after: 24:00:00, index", Set.empty), //The value of the first directive is invalid and contains colons.
      ("unavailable_after: 24:00:00, MyBot: index", Set(Index))
    )

    forEvery(table)((input, expectedResult) => {
      val parser = RobotsTagParsingService(Set("MyBot"))
      parser.parse(input)
      assertResult(expectedResult)(parser.getDirectives)
    })
  }

  "RobotsTagParsingService (without target user agents)" should "collect directives that apply to all user agents" in {
    val inputs = Seq(
      "index",
      "index, UnknownBot: follow",
      "index, UnknownBot: follow, nocache"
    )

    inputs.foreach(input => {
      val parser = RobotsTagParsingService()
      parser.parse(input)
      assertResult(Set(Index))(parser.getDirectives)
    })
  }

  it should "not collect directives that only apply to specific user agents" in {
    val inputs = Seq(
      "UnknownBot: index",
      "UnknownBot-1: index, UnknownBot-2: index"
    )

    inputs.foreach(input => {
      val parser = RobotsTagParsingService()
      parser.parse(input)
      assert(parser.getDirectives.isEmpty)
    })
  }

  "RobotsTagParsingService (with target user agents)" should "collect directives that apply to all user agents" in {
    val inputs = Seq(
      "index",
      "index, UnknownBot: follow",
      "index, UnknownBot: follow, nocache"
    )

    val parser = RobotsTagParsingService(Set("MyBot-1", "MyBot-2"))

    inputs.foreach(input => {
      parser.parse(input)
      assertResult(Set(Index))(parser.getDirectives)
    })

    parser.parse("index, MyBot-1: follow")
    assert(parser.getDirectives.contains(Index))
  }

  it should "collect directives that apply to the target user agents" in {
    val parser = RobotsTagParsingService(Set("MyBot-1", "MyBot-2"))

    parser.parse("MyBot-1: index")
    assertResult(Set(Index))(parser.getDirectives)

    parser.parse("MyBot-2: follow")
    assertResult(Set(Index, Follow))(parser.getDirectives)

    val inputs = Seq(
      "MyBot-1: index, follow",
      "MyBot-1: index, MyBot-2: follow",
      "UnknownBot: nocache, MyBot-1: index, follow",
      "MyBot-1: index, UnknownBot: all, MyBot-2: follow, UnknownBot: nocache"
    )

    inputs.foreach(input => {
      parser.reset()
      parser.parse(input)
      assertResult(Set(Index, Follow))(parser.getDirectives)
    })
  }

  it should "not collect directives that only apply to other user agents" in {
    val inputs = Seq(
      "UnknownBot: index",
      "UnknownBot: index, follow",
      "UnknownBot-1: index, UnknownBot-2: index",
      "UnknownBot-1: index, follow, UnknownBot-2: index, follow"
    )

    val parser = RobotsTagParsingService(Set("MyBot-1", "MyBot-2"))
    inputs.foreach(parser.parse)
    assert(parser.getDirectives.isEmpty)
  }

  it should "perform case-insensitive user agent matching" in {
    val parser = RobotsTagParsingService(Set("MyBot"))
    parser.parse("mybot: index")
    assertResult(Set(Index))(parser.getDirectives)
  }

  it should "work with empty user agent groups" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("MyBot:", Set.empty),
      ("UnknownBot:", Set.empty),
      ("index, MyBot:", Set(Index)),
      ("index, UnknownBot:", Set(Index)),
      ("MyBot: MyBot: index", Set(Index)),
      ("MyBot: UnknownBot: index", Set.empty),
      ("UnknownBot: MyBot: index", Set(Index))
    )

    forEvery(table)((input, expectedResult) => {
      val parser = RobotsTagParsingService(Set("MyBot"))
      parser.parse(input)
      assertResult(expectedResult)(parser.getDirectives)
    })
  }

  it should "parse complex ambiguous directive strings" in {
    val unavailableAfter = Directive("unavailable_after", ZonedDateTime.of(2025, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC).toInstant)

    val table = Table(
      ("Input", "Expected Result"),
      ("max-image-preview: large, unavailable_after: Wed, 31 Dec 2025 23:59:59 GMT, index, follow", Set(MaxImagePreview, unavailableAfter, Index, Follow)),
      ("UnknownBot: foo, MyBot: index, UnknownBot: bar: 100, baz: 200, MyBot: max-image-preview: large, UnknownBot: foo, bar, baz, MyBot: unavailable_after: Wed, 31 Dec 2025 23:59:59 GMT", Set(Index, MaxImagePreview, unavailableAfter)),
      ("UnknownBot: MyBot: unavailable_after: Wed, 31 Dec 2025 23:59:59 GMT, max-image-preview: large, index", Set(unavailableAfter, MaxImagePreview, Index)),
      ("index, foo: bar, max-image-preview: large, MyBot: follow", Set(Index, Follow))
    )

    forEvery(table)((input, expectedResult) => {
      val parser = RobotsTagParsingService(Set("MyBot"))
      parser.parse(input)
      assertResult(expectedResult)(parser.getDirectives)
    })
  }
}
