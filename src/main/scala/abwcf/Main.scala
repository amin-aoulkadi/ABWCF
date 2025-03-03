package abwcf

import abwcf.actors.Crawler
import org.apache.pekko.actor.typed.ActorSystem

val seedUrls = Seq("https://www.oth-regensburg.de/", "https://example.com/")

@main def startCrawler(): Unit = {
  val actorSystem = ActorSystem(Crawler(), "crawler")

  actorSystem ! Crawler.SeedUrls(seedUrls)
}
