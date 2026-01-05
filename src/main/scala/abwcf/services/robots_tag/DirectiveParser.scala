package abwcf.services.robots_tag

trait DirectiveParser[T] {
  /**
   * Parses the first directive (and its value, if applicable) from a [[PreprocessedString]].
   *
   * @param input the [[PreprocessedString]] to process
   * @return the parsed directive and the input string without the parsed directive and its value
   * @throws Exception if any exceptions are thrown while parsing
   */
  def parse(input: PreprocessedString): ParserResult[Directive[T]]
}
