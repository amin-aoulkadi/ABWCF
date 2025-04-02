package abwcf

enum PageStatus {
  case Discovered, Processed
}

case class PageEntity( //TODO: Page
                       url: String,
                       status: PageStatus,
                       crawlDepth: Int,
                       crawlPriority: Int
                     )
