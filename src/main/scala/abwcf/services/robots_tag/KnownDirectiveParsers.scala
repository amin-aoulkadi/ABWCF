package abwcf.services.robots_tag

import abwcf.services.robots_tag.parsers.*

object KnownDirectiveParsers {
  case class NamedDirectiveParser[T](name: String, parser: DirectiveParser[T])

  /*
    Different vendors support different directives. Vendors may also stop supporting directives that they previously supported.
    The ability to parse directives that are no longer supported by some vendor(s) is useful, because such directives can still be encountered while crawling.

    Sources and references:
      - Bing: https://www.bing.com/webmasters/help/which-robots-metatags-does-bing-support-5198d240
      - Google: https://developers.google.com/search/docs/crawling-indexing/robots-meta-tag
      - HTML 4: https://www.w3.org/TR/html4/appendix/notes.html#h-B.4.1.2
   */

  /**
   * Specified by: Google, HTML 4
   */
  val All = NamedDirectiveParser("all", SimpleDirectiveParser)

  /**
   * Specified by: TODO
   */
  val Follow = NamedDirectiveParser("follow", SimpleDirectiveParser)

  /**
   * Specified by: HTML 4
   */
  val Index = NamedDirectiveParser("index", SimpleDirectiveParser)

  /**
   * Specified by: Google
   */
  val IndexIfEmbedded = NamedDirectiveParser("indexifembedded", SimpleDirectiveParser)

  /**
   * Specified by: Bing, Google
   */
  val MaxImagePreview = NamedDirectiveParser("max-image-preview", StringDirectiveParser)

  /**
   * Specified by: Bing, Google
   */
  val MaxSnippet = NamedDirectiveParser("max-snippet", IntDirectiveParser)

  /**
   * Specified by: Bing, Google
   */
  val MaxVideoPreview = NamedDirectiveParser("max-video-preview", IntDirectiveParser)

  /**
   * Specified by: Bing, Google (deprecated)
   */
  val NoArchive = NamedDirectiveParser("noarchive", SimpleDirectiveParser)

  /**
   * Specified by: Bing
   */
  val NoCache = NamedDirectiveParser("nocache", SimpleDirectiveParser)

  /**
   * Specified by: Google, HTML 4
   */
  val NoFollow = NamedDirectiveParser("nofollow", SimpleDirectiveParser)

  /**
   * Specified by: Google
   */
  val NoImageIndex = NamedDirectiveParser("noimageindex", SimpleDirectiveParser)

  /**
   * Specified by: Bing, Google, HTML 4
   */
  val NoIndex = NamedDirectiveParser("noindex", SimpleDirectiveParser)

  /**
   * Specified by: Google (deprecated)
   */
  val NoSitelinksSearchBox = NamedDirectiveParser("nositelinkssearchbox", SimpleDirectiveParser)

  /**
   * Specified by: Bing, Google
   */
  val NoSnippet = NamedDirectiveParser("nosnippet", SimpleDirectiveParser)

  /**
   * Specified by: Google
   */
  val NoTranslate = NamedDirectiveParser("notranslate", SimpleDirectiveParser)

  /**
   * Specified by: Google
   */
  val None = NamedDirectiveParser("none", SimpleDirectiveParser)

  /**
   * Specified by: Google
   */
  val UnavailableAfter = NamedDirectiveParser("unavailable_after", TemporalDirectiveParser)

  val DefaultParsers = Seq(
    All,
    Follow,
    Index,
    IndexIfEmbedded,
    MaxImagePreview,
    MaxSnippet,
    MaxVideoPreview,
    NoArchive,
    NoCache,
    NoFollow,
    NoImageIndex,
    NoIndex,
    NoSitelinksSearchBox,
    NoSnippet,
    NoTranslate,
    None,
    UnavailableAfter
  )

  val DefaultParsersByName: Map[String, DirectiveParser[?]] = DefaultParsers.map(ndp => (ndp.name, ndp.parser)).toMap
}
