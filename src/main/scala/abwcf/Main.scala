package abwcf

import abwcf.actors.Crawler
import org.apache.pekko.actor.typed.ActorSystem

val seedUrls = Seq("https://www.oth-regensburg.de/", "https://example.com/", "https://example.com/1", "https://example.com/2", "https://example.com/2")

@main def startCrawler(): Unit = {
  val actorSystem = ActorSystem(Crawler(), "crawler")

  actorSystem ! Crawler.SeedUrls(seedUrls)
}
