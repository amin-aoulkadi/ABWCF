package abwcf.util

import abwcf.services.robots_tag.DirectiveCollection
import abwcf.services.robots_tag.KnownSimpleDirectives.{Follow, NoFollow}

object RobotsUtils {
  /**
   * Determines whether a set of robots (meta) tag directives permits following links contained within the associated resource.
   * 
   * @param directiveCollection the robots (meta) tag directives
   * @return `true` if links can be followed, otherwise `false`
   */
  def canFollowLinks(directiveCollection: DirectiveCollection): Boolean = {
    val all = directiveCollection.withoutUserAgent.toSet
    val target = directiveCollection.withUserAgent.view.values
    val allFollow = all.contains(Follow)
    val allNoFollow = all.contains(NoFollow)
    
    if (target.isEmpty) {
      !allNoFollow || allFollow
    } else {
      if (allNoFollow && !allFollow) { //Links should not be followed unless there is a "follow" directive for at least one target user agent.
        target.exists(_.contains(Follow))
      } else { //Links can be followed if there is at least one target user agent without a "nofollow" directive.
        target.exists(directives => !directives.contains(NoFollow) || directives.contains(Follow))
      }
    }
  }
}
