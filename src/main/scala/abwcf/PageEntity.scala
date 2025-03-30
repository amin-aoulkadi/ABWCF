package abwcf

enum PageStatus {
  case Unknown, Discovered, Processed
}

case class PageEntity(
                      url: String,
                      status: PageStatus,
                      crawlDepth: Int
                    )
