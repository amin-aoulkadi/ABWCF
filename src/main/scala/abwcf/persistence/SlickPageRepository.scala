package abwcf.persistence

import abwcf.data.{Page, PageStatus}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.slick.scaladsl.{Slick, SlickSession}
import org.apache.pekko.stream.scaladsl.Sink
import slick.jdbc.GetResult

import scala.concurrent.Future

class SlickPageRepository(implicit val materializer: Materializer) extends PageRepository {
  private implicit val session: SlickSession = SlickSessionContainer.getSession
  import session.profile.api.*

  /**
   * Converts a result set to a [[Page]] instance.
   */
  private implicit val getPageResult: GetResult[Page] = GetResult(r => Page(r.<<, PageStatus.valueOf(r.<<), r.<<, r.<<))

  override def insert(page: Page): Future[Int] = {
    val query = sqlu"""INSERT INTO pages VALUES (${page.url}, ${page.status.toString}, ${page.crawlDepth}, ${page.crawlPriority})"""
    session.db.run(query)
  }

  override def updateStatus(url: String, status: PageStatus): Future[Int] = {
    val query = sqlu"""UPDATE pages SET status = ${status.toString} WHERE url = $url"""
    session.db.run(query)
  }

  override def findByUrl(url: String): Future[Option[Page]] = {
    val query = sql"""SELECT * FROM pages WHERE url = $url""".as[Page]
    Slick.source(query).runWith(Sink.headOption)
  }

  override def findByStatusOrderByCrawlPriorityDesc(status: PageStatus, limit: Int): Future[Seq[Page]] = {
    val query = sql"""SELECT * FROM pages WHERE status = ${status.toString} ORDER BY crawl_priority DESC LIMIT $limit""".as[Page]
    Slick.source(query).runWith(Sink.seq)
  }
}
