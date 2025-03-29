package abwcf

enum PageStatus {
  case Discovered, Processed
}

case class PageEntity(
                      url: String,
                      status: PageStatus,
                      crawlDepth: Int
                    )
