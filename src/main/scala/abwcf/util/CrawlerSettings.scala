package abwcf.util

/**
 * API for providing user-defined code to the ABWCF.
 *
 * @param prioritizationFunction A function that assigns a crawl priority to a [[abwcf.data.PageCandidate]]. The default is [[PrioritizationFunctions.Random]].
 */
case class CrawlerSettings(prioritizationFunction: PrioritizationFunction = PrioritizationFunctions.Random) {
  def withPrioritizationFunction(prioritizationFunction: PrioritizationFunction): CrawlerSettings = copy(prioritizationFunction = prioritizationFunction)
}
