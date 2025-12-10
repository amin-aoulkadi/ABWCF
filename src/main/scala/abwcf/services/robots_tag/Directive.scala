package abwcf.services.robots_tag

case class Directive[T](name: String, value: Option[T] = None) {
  def hasValue: Boolean =
    value.nonEmpty
}

object Directive {
  def apply[T](name: String, value: T): Directive[T] =
    new Directive(name, Option(value))
}
