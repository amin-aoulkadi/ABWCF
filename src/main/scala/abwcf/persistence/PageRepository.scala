package abwcf.persistence

import abwcf.{PageEntity, PageStatus}

import scala.concurrent.Future

trait PageRepository {
  def insert(page: PageEntity): Future[Int]
  def update(page: PageEntity): Future[Int]
  def updateStatus(url: String, status: PageStatus): Future[Int]
  def findByUrl(url: String): Future[Option[PageEntity]]
  def findByStatus(status: PageStatus): Future[Seq[PageEntity]]
}
