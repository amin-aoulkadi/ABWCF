package abwcf.util

import abwcf.data.PageCandidate

import java.util.concurrent.ThreadLocalRandom

object PrioritizationFunctions {
  /**
   * Generates pseudo-random crawl priorities.
   */
  def random: Long =
    ThreadLocalRandom.current().nextLong()

  /**
   * Biases the crawler towards breadth-first crawling.
   */
  def breadthFirst(candidate: PageCandidate): Long =
    -candidate.crawlDepth

  /**
   * Biases the crawler towards depth-first crawling.
   */
  def depthFirst(candidate: PageCandidate): Long =
    candidate.crawlDepth
}
