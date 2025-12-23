package abwcf.services.robots_tag

object KnownUserAgents {
  val DefaultUserAgents = Set(
    //Google:
    //https://developers.google.com/crawling/docs/crawlers-fetchers/google-common-crawlers:
    "Googlebot",
    "Googlebot-Image",
    "Googlebot-Video",
    "Googlebot-News",
    "Storebot-Google",
    "Google-InspectionTool",
    "GoogleOther",
    "GoogleOther-Image",
    "GoogleOther-Video",
    "Google-CloudVertexBot",
    "Google-Extended",
    //https://developers.google.com/crawling/docs/crawlers-fetchers/google-special-case-crawlers:
    "APIs-Google",
    "AdsBot-Google-Mobile",
    "AdsBot-Google",
    "Mediapartners-Google",
    "DuplexWeb-Google", //Deprecated
    "AdsBot-Google-Mobile-Apps", //Deprecated
    "googleweblight" //Deprecated
  )
}
