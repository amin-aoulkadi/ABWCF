package abwcf.persistence

import abwcf.data.{Page, PageStatus}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.slick.scaladsl.{Slick, SlickSession}
import org.apache.pekko.stream.scaladsl.Sink

import scala.concurrent.Future

class SlickPageRepository(implicit val session: SlickSession, val materializer: Materializer) extends PageRepository {
  import session.profile.api.*

  private implicit val statusMapper: BaseColumnType[PageStatus] = MappedColumnType.base[PageStatus, String](
    status => status.toString,
    string => PageStatus.valueOf(string)
  )

  private class PageTable(tag: Tag) extends Table[Page](tag, None, "pages") {
    def url = column[String]("url")
    def status = column[PageStatus]("status")
    def crawlDepth = column[Int]("crawl_depth")
    def crawlPriority = column[Int]("crawl_priority")

    override def * = (url, status, crawlDepth, crawlPriority).mapTo[Page]
  }

  private lazy val pages = TableQuery[PageTable]

  override def insert(page: Page): Future[Int] = {
    session.db.run(pages += page)
  }

  override def updateStatus(url: String, status: PageStatus): Future[Int] = {
    val query = pages
      .filter(_.url === url)
      .map(_.status)
      .update(status)

    session.db.run(query)
  }

  override def findByUrl(url: String): Future[Option[Page]] = {
    val query = pages.filter(_.url === url).result
    Slick.source(query).runWith(Sink.headOption)
  }

  override def findByStatusOrderByCrawlPriorityDesc(status: PageStatus, limit: Int): Future[Seq[Page]] = {
    val query = pages
      .filter(_.status === status)
      .sortBy(_.crawlPriority.desc)
      .take(limit)
      .result

    Slick.source(query).runWith(Sink.seq)
  }
}
