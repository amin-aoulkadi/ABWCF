package abwcf.persistence

import abwcf.data.{Page, PageStatus}

import scala.concurrent.Future

trait PageRepository {
  def insert(pages: Iterable[Page]): Future[Array[Int]]
  def updateStatus(batch: Iterable[(String, PageStatus)]): Future[Array[Int]]
  def findByUrl(url: String): Future[Option[Page]]
  def findByStatusOrderByCrawlPriorityDesc(status: PageStatus, limit: Int): Future[Seq[Page]]
}
