package abwcf.persistence

import abwcf.{PageEntity, PageStatus}
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

  private class PageTable(tag: Tag) extends Table[PageEntity](tag, None, "pages") {
    def url = column[String]("url")
    def status = column[PageStatus]("status")
    def crawlDepth = column[Int]("crawl_depth")

    override def * = (url, status, crawlDepth).mapTo[PageEntity]
  }

  private lazy val pages = TableQuery[PageTable]

  override def insert(page: PageEntity): Future[Int] = {
    session.db.run(pages += page)
  }

  override def updateStatus(url: String, status: PageStatus): Future[Int] = {
    val query = pages
      .filter(_.url === url)
      .map(_.status)
      .update(status)

    session.db.run(query)
  }

  override def findByUrl(url: String): Future[Option[PageEntity]] = {
    val query = pages.filter(_.url === url).result
    Slick.source(query).runWith(Sink.headOption)
  }

  override def findByStatus(status: PageStatus, limit: Int): Future[Seq[PageEntity]] = {
    val query = pages
      .filter(_.status === status)
      .take(limit)
      .result

    Slick.source(query).runWith(Sink.seq)
  }
}
