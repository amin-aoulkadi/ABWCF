package abwcf.services.robots_tag

import abwcf.services.robots_tag.parsers.*

object KnownDirectiveParsers {
  case class NamedDirectiveParser[T](name: String, parser: DirectiveParser[T])

  /*
    Different vendors support different directives. Vendors may also stop supporting directives that they previously supported.
    The ability to parse directives that are no longer supported by some vendor(s) is useful, because such directives can still be encountered while crawling.

    Sources and references:
      - Apple: https://support.apple.com/en-us/119829
      - Baidu: https://www.baidu.com/search/robots_english.html
      - Bing: https://www.bing.com/webmasters/help/which-robots-metatags-does-bing-support-5198d240
      - DeviantArt: https://www.deviantart.com/team/journal/UPDATE-All-Deviations-Are-Opted-Out-of-AI-Datasets-934500371
      - Google: https://developers.google.com/search/docs/crawling-indexing/robots-meta-tag
      - HTML 4: https://www.w3.org/TR/html4/appendix/notes.html#h-B.4.1.2 (first published in 1997)
      - Yandex: https://yandex.com/support/webmaster/en/controlling-robot/metatags
      - robotstxt.org: https://www.robotstxt.org/metabof.html (report from a 1996 workshop)
   */

  /**
   * Specified by: Apple, Google, HTML 4, Yandex, robotstxt.org
   */
  val All = NamedDirectiveParser("all", SimpleDirectiveParser)

  /**
   * Specified by: Yandex
   */
  val Archive = NamedDirectiveParser("archive", SimpleDirectiveParser)

  /**
   * Specified by: Yandex, robotstxt.org
   */
  val Follow = NamedDirectiveParser("follow", SimpleDirectiveParser)

  /**
   * Specified by: HTML 4, Yandex, robotstxt.org
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
   * Specified by: DeviantArt
   */
  val NoAi = NamedDirectiveParser("noai", SimpleDirectiveParser)

  /**
   * Specified by: Baidu, Bing, Google (deprecated), Yandex
   */
  val NoArchive = NamedDirectiveParser("noarchive", SimpleDirectiveParser)

  /**
   * Specified by: Bing
   */
  val NoCache = NamedDirectiveParser("nocache", SimpleDirectiveParser)

  /**
   * Specified by: Apple, Baidu, Google, HTML 4, Yandex, robotstxt.org
   */
  val NoFollow = NamedDirectiveParser("nofollow", SimpleDirectiveParser)

  /**
   * Specified by: DeviantArt
   */
  val NoImageAi = NamedDirectiveParser("noimageai", SimpleDirectiveParser)

  /**
   * Specified by: Google
   */
  val NoImageIndex = NamedDirectiveParser("noimageindex", SimpleDirectiveParser)

  /**
   * Specified by: Apple, Bing, Google, HTML 4, Yandex, robotstxt.org
   */
  val NoIndex = NamedDirectiveParser("noindex", SimpleDirectiveParser)

  /**
   * Specified by: Google (deprecated)
   */
  val NoSitelinksSearchBox = NamedDirectiveParser("nositelinkssearchbox", SimpleDirectiveParser)

  /**
   * Specified by: Apple, Bing, Google
   */
  val NoSnippet = NamedDirectiveParser("nosnippet", SimpleDirectiveParser)

  /**
   * Specified by: Google
   */
  val NoTranslate = NamedDirectiveParser("notranslate", SimpleDirectiveParser)

  /**
   * Specified by: Apple, Google, Yandex, robotstxt.org
   */
  val None = NamedDirectiveParser("none", SimpleDirectiveParser)

  /**
   * Specified by: Google
   */
  val UnavailableAfter = NamedDirectiveParser("unavailable_after", TemporalDirectiveParser)

  val DefaultParsers = Seq(
    All,
    Archive,
    Follow,
    Index,
    IndexIfEmbedded,
    MaxImagePreview,
    MaxSnippet,
    MaxVideoPreview,
    NoAi,
    NoArchive,
    NoCache,
    NoFollow,
    NoImageAi,
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
