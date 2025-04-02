package abwcf

/**
 * A URL that has not been normalized, filtered, deduplicated and prioritized yet.
 * 
 * A normalized, filtered, deduplicated and prioritized [[PageCandidate]] becomes a [[Page]].
 * 
 * [[PageCandidate]]s are not persisted in the database.
 */
case class PageCandidate(url: String, crawlDepth: Int)
