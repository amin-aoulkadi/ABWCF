package abwcf

enum PageStatus {
  case Discovered, Processed
}

/**
 * A page that is currently being crawled or has already been crawled.
 * 
 * [[Page]]s are persisted in the database.
 */
case class Page(
                 url: String,
                 status: PageStatus,
                 crawlDepth: Int,
                 crawlPriority: Int
               )
