package abwcf.services.robots_tag

import scala.collection.{MapView, View, mutable}

/**
 * All user-facing methods are defined by the [[DirectiveCollection]] trait. Users can only query the collection, they can not to modify it.
 */
class ModifiableDirectiveCollection extends DirectiveCollection {
  private val allDirectives = All()
  private val directivesWithUserAgent = WithUserAgent()
  private val directivesWithoutUserAgent = WithoutUserAgent()

  /**
   * Adds a directive that applies to all user agents.
   */
  def addDirective(directive: Directive[?]): Unit = {
    allDirectives.directives.add(directive)
    directivesWithoutUserAgent.directives.add(directive)
  }

  /**
   * Adds a directive that only applies to a specific user agent.
   *
   * The user agent must be trimmed and lowercased.
   */
  def addDirective(userAgent: String, directive: Directive[?]): Unit = {
    allDirectives.directives.add(directive)

    directivesWithUserAgent.directivesByUserAgent.updateWith(userAgent)({
      case Some(directives) =>
        directives.add(directive)
        Some(directives)

      case None => Some(mutable.Set(directive))
    })
  }

  def clear(): Unit = {
    allDirectives.directives.clear()
    directivesWithUserAgent.directivesByUserAgent.clear()
    directivesWithoutUserAgent.directives.clear()
  }

  override def all: AllDirectives =
    allDirectives

  override def withUserAgent: DirectivesWithUserAgent =
    directivesWithUserAgent

  override def withoutUserAgent: DirectivesWithoutUserAgent =
    directivesWithoutUserAgent

  override def isEmpty: Boolean =
    allDirectives.directives.isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

  private class All extends AllDirectives {
    val directives = mutable.Set.empty[Directive[?]]

    override def toSet: Set[Directive[?]] =
      Set.from(directives) //Creates an immutable copy.

    override def view: View[Directive[?]] =
      directives.view
  }

  private class WithUserAgent extends DirectivesWithUserAgent {
    val directivesByUserAgent = mutable.Map.empty[String, mutable.Set[Directive[?]]]

    override def toSet: Set[Directive[?]] =
      Set.from(directivesByUserAgent.view.values.flatten) //Creates an immutable copy.

    override def toMap: Map[String, Set[Directive[?]]] =
      Map.from(view) //Creates an immutable copy.

    override def view: MapView[String, Set[Directive[?]]] =
      directivesByUserAgent.view.mapValues(Set.from)
  }

  private class WithoutUserAgent extends DirectivesWithoutUserAgent {
    val directives = mutable.Set.empty[Directive[?]]

    override def toSet: Set[Directive[?]] =
      Set.from(directives) //Creates an immutable copy.

    override def view: View[Directive[?]] =
      directives.view
  }

  override def equals(other: Any): Boolean = other match {
    case that: ModifiableDirectiveCollection =>
      this.directivesWithUserAgent == that.directivesWithUserAgent
      && this.directivesWithoutUserAgent == that.directivesWithoutUserAgent

    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(directivesWithUserAgent, directivesWithoutUserAgent)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
