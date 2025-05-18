package abwcf.persistence

import abwcf.data.{Page, PageStatus}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.slick.scaladsl.{Slick, SlickSession}
import org.apache.pekko.stream.scaladsl.Sink
import slick.jdbc.GetResult

import scala.concurrent.Future

class SlickPageRepository(using session: SlickSession, materializer: Materializer) extends PageRepository {
  import session.profile.api.*

  /**
   * Converts a result set to a [[Page]] instance.
   */
  private given getPageResult: GetResult[Page] = GetResult(r => Page(r.<<, PageStatus.valueOf(r.<<), r.<<, r.<<))

  override def insert(batch: Iterable[Page]): Future[Array[Int]] = {
    val query = SimpleDBIO(context => {
      val statement = context.connection.prepareStatement("INSERT INTO pages VALUES (?, ?, ?, ?)")

      batch.foreach(page => {
        statement.setString(1, page.url)
        statement.setString(2, page.status.toString)
        statement.setInt(3, page.crawlDepth)
        statement.setLong(4, page.crawlPriority)
        statement.addBatch()
      })

      statement.executeBatch()
    })

    session.db.run(query)
  }
  
  override def updateStatus(batch: Iterable[(String, PageStatus)]): Future[Array[Int]] = {
    val query = SimpleDBIO(context => {
      val statement = context.connection.prepareStatement("UPDATE pages SET status = ? WHERE url = ?")
      
      batch.foreach((url, status) => {
        statement.setString(1, status.toString)
        statement.setString(2, url)
        statement.addBatch()
      })
      
      statement.executeBatch()
    })
    
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
