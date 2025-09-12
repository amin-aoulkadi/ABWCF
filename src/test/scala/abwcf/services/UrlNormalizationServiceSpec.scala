package abwcf.services

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.Tables.Table

//noinspection HttpUrlsUsage
class UrlNormalizationServiceSpec extends AnyFlatSpec with TableDrivenPropertyChecks {
  val urlNormalizationService = new UrlNormalizationService(true, true, true)

  def test(input: String, expectedResult: String): Unit = {
    val result = urlNormalizationService.normalize(input)
    assert(result.toString.equals(expectedResult))
  }

  "UrlNormalizationService" should "not change URLs that are already in normal form" in {
    test("https://example.com/", "https://example.com/")
    test("https://example.com/abc/def/ghi", "https://example.com/abc/def/ghi")
  }

  it should "convert scheme and host to lowercase" in {
    test("HTTPS://EXAMPLE.COM/ABC", "https://example.com/ABC")
  }

  it should "work with IP addresses" in {
    test("https://127.0.0.1/", "https://127.0.0.1/")
    test("https://[::1]/", "https://[::1]/")
  }

  it should "normalize ports" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("https://example.com:/", "https://example.com/"),
      ("http://example.com:80/", "http://example.com/"), //HTTP with default port
      ("http://example.com:1234/", "http://example.com:1234/"),
      ("https://example.com:443/", "https://example.com/"), //HTTPS with default port
      ("https://example.com:1234/", "https://example.com:1234/")
    )

    forEvery(table)(test)
  }

  it should "normalize paths" in {
    test("https://example.com", "https://example.com/")
    test("https://example.com/./abc//def/..///ghi", "https://example.com/abc/ghi")
  }

  it should "be able to remove user information, query and fragment components" in {
    test("https://user:password@example.com/?key=value#fragment", "https://example.com/")
  }

  it should "work with percent-encoded characters" in {
    test("https://user@example.com/abc/%3F%23%2F%5B%5D/def?%3F%23%2F%5B%5D#%3F%23%2F%5B%5D", "https://example.com/abc/%3F%23%2F%5B%5D/def")
  }

  it should "work with internationalized domain names" in {
    test("https://aäeéoöuü.example/", "https://xn--aeou-loa5a0g3b.example/")
    test("https://😃.example/", "https://xn--h28h.example/")
    test("https://user@😃.example/", "https://xn--h28h.example/")
  }
}
