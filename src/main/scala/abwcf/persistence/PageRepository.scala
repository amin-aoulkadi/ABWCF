package abwcf.persistence

import abwcf.{PageEntity, PageStatus}

import scala.concurrent.Future

trait PageRepository { //TODO: Remove unused queries.
  def insert(page: PageEntity): Future[Int]
  def update(page: PageEntity): Future[Int]
  def updateStatus(url: String, status: PageStatus): Future[Int]
  def findByUrl(url: String): Future[Option[PageEntity]]
  def findByStatus(status: PageStatus, limit: Int): Future[Seq[PageEntity]]
}
