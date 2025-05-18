package abwcf.persistence

import abwcf.data.HostInformation

import scala.concurrent.Future

trait HostRepository {
  def insert(batch: Iterable[HostInformation]): Future[Array[Int]]
  def update(hostInfo: HostInformation): Future[Int]
  def findBySchemeAndAuthority(schemeAndAuthority: String): Future[Option[HostInformation]]
}
