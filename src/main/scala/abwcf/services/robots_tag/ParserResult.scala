package abwcf.services.robots_tag

/**
 * A parsed value and the remainder of the string it was parsed from (i.e. the original string that the value was parsed from, but without the parsed value).
 */
case class ParserResult[+T](value: T, remainder: String)
