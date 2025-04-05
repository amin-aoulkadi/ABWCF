package abwcf.util

import abwcf.data.PageCandidate

/**
 * A function that assigns a crawl priority to a [[PageCandidate]].
 *
 * Pages with a high crawl priority are more likely to be crawled than pages with a low crawl priority.
 */
type PrioritizationFunction = PageCandidate => Long

object PrioritizationFunctions {
  /**
   * Generates pseudo-random crawl priorities.
   */
  val Random: PrioritizationFunction =
    _ => scala.util.Random.nextLong()

  /**
   * Biases the crawler towards breadth-first crawling.
   */
  val BreadthFirst: PrioritizationFunction =
    candidate => -candidate.crawlDepth

  /**
   * Biases the crawler towards depth-first crawling.
   */
  val DepthFirst: PrioritizationFunction =
    candidate => candidate.crawlDepth
}
