package abwcf.services.robots_tag

import scala.collection.{MapView, View}

trait DirectiveCollection {
  /**
   * Returns all directives within this collection.
   */
  def all: AllDirectives

  /**
   * Returns directives that only apply to specific user agents.
   *
   * Directives that apply to all user agents (see [[withoutUserAgent]]) are not included.
   *
   * @note If this [[DirectiveCollection]] was populated by a [[RobotsMetaParsingService]] or a [[RobotsTagParsingService]], then every directive returned by this method applies to one of the target user agents of the parser.
   */
  def withUserAgent: DirectivesWithUserAgent

  /**
   * Returns directives that apply to all user agents.
   */
  def withoutUserAgent: DirectivesWithoutUserAgent

  def isEmpty: Boolean
  def nonEmpty: Boolean

  trait AllDirectives {
    def toSet: Set[Directive[?]]
    def view: View[Directive[?]]
  }

  trait DirectivesWithUserAgent {
    def toSet: Set[Directive[?]]

    /**
     * '''Key:''' The trimmed and lowercased user agent.<br>
     * '''Value:''' Directives that only apply to the user agent.
     */
    def toMap: Map[String, Set[Directive[?]]]

    /**
     * '''Key:''' The trimmed and lowercased user agent.<br>
     * '''Value:''' Directives that only apply to the user agent.
     */
    def view: MapView[String, Set[Directive[?]]]
  }

  trait DirectivesWithoutUserAgent {
    def toSet: Set[Directive[?]]
    def view: View[Directive[?]]
  }
}
