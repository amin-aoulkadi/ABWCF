package abwcf.util

import abwcf.services.robots_tag.KnownSimpleDirectives.{Follow, Index, NoFollow}
import abwcf.services.robots_tag.ModifiableDirectiveCollection
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks

class RobotsUtilsSpec extends AnyFlatSpec with TableDrivenPropertyChecks {
  "canFollowLinks()" should "determine whether links can be followed" in {
    val follow = Seq(Follow)
    val noFollow = Seq(NoFollow)
    val both = Seq(Follow, NoFollow)
    val unrelated = Seq(Index)
    val empty = Seq.empty

    val table = Table(
      ("Directives (All User Agents)", "Directives (Target User Agent 1)", "Directives (Target User Agent 2)", "Expected Result"),
      (both, both, both, true),
      (both, both, unrelated, true),
      (both, empty, empty, true),
      (empty, empty, empty, true),
      (empty, noFollow, noFollow, false),
      (empty, noFollow, unrelated, true),
      (follow, follow, follow, true),
      (follow, noFollow, noFollow, false),
      (follow, noFollow, unrelated, true),
      (noFollow, both, unrelated, true),
      (noFollow, empty, empty, false),
      (noFollow, follow, follow, true),
      (noFollow, follow, unrelated, true),
      (noFollow, noFollow, noFollow, false),
      (noFollow, unrelated, unrelated, false),
      (unrelated, empty, empty, true),
      (unrelated, noFollow, noFollow, false),
      (unrelated, unrelated, noFollow, true),
      (unrelated, unrelated, unrelated, true)
    )

    forEvery(table)((directivesAll, directivesTarget1, directivesTarget2, expectedResult) => {
      val directiveCollection = ModifiableDirectiveCollection()

      directivesAll.foreach(directiveCollection.addDirective)
      directivesTarget1.foreach(directiveCollection.addDirective("mybot-1", _))
      directivesTarget2.foreach(directiveCollection.addDirective("mybot-2", _))

      assertResult(expectedResult)(RobotsUtils.canFollowLinks(directiveCollection))
    })
  }
}
