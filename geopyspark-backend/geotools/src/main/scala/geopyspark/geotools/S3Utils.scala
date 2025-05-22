package geopyspark.geotools

import geotrellis.store.s3._

import org.apache.spark._

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{ListObjectsV2Request, ListObjectsV2Response}

import java.net.URI

import scala.collection.JavaConverters._
import scala.collection.mutable


object S3Utils {
  def listKeys(uris: Array[URI], extensions: Seq[String], s3Client: S3Client): Array[String] =
    uris.flatMap { uri => listKeys(uri, extensions, s3Client) }

  def listKeys(uri: URI, extensions: Seq[String], s3Client: S3Client): Array[String] =
    listKeys(uri.getHost, uri.getPath.tail, extensions, s3Client)

  def listKeys(
    s3bucket: String,
    s3prefix: String,
    extensions: Seq[String],
    s3Client: S3Client
  ): Array[String] = {
    val objectRequest = ListObjectsV2Request
      .builder()
      .bucket(s3bucket)
      .prefix(s3prefix)

    listKeys(objectRequest, s3Client)
      .filter { path => extensions.exists { e => path.endsWith(e) } }
      .collect { case key =>
        s"https://s3.amazonaws.com/${s3bucket}/${key}"
      }.toArray
  }

  // Copied from GeoTrellis codebase
  def listKeys(listObjectsRequestBuilder: ListObjectsV2Request.Builder, s3Client: S3Client): Array[String] = {
    var listing: ListObjectsV2Response = null
    val result = mutable.ListBuffer[String]()
    do {
      listing = s3Client.listObjectsV2(listObjectsRequestBuilder.build())
      // avoid including "directories" in the input split, can cause 403 errors on GET
      result ++= listing.contents().asScala.map(_.key()).filterNot(_ endsWith "/")
      listObjectsRequestBuilder.continuationToken(listing.nextContinuationToken())
    } while (listing.isTruncated)

    result.toArray
  }
}
