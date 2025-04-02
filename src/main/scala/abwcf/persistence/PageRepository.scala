package abwcf.persistence

import abwcf.{Page, PageStatus}

import scala.concurrent.Future

trait PageRepository {
  def insert(page: Page): Future[Int]
  def updateStatus(url: String, status: PageStatus): Future[Int]
  def findByUrl(url: String): Future[Option[Page]]
  def findByStatusOrderByCrawlPriorityDesc(status: PageStatus, limit: Int): Future[Seq[Page]]
}
