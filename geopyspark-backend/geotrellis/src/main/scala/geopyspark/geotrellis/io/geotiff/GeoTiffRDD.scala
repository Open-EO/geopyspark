package geopyspark.geotrellis.io.geotiff

import geopyspark.geotrellis._
import geotrellis.proj4._
import geotrellis.store.avro._
import geotrellis.spark.store.hadoop._
import geotrellis.spark.store.s3._
import geotrellis.store.s3._

import scala.collection.JavaConversions._
import java.net.URI

import scala.reflect._
import software.amazon.awssdk.services.s3.S3Client
import org.apache.spark._
import org.apache.hadoop.fs.Path
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}


object GeoTiffRDD {
  import Constants._

  object HadoopGeoTiffRDDOptions {
    def default = HadoopGeoTiffRDD.Options.DEFAULT

    def setValues(
      intMap: Map[String, Int],
      stringMap: Map[String, String],
      partitionBytes: Option[Long]
    ): HadoopGeoTiffRDD.Options = {
      val crs: Option[CRS] =
        if (stringMap.contains("crs"))
          TileLayer.getCRS(stringMap("crs"))
        else
          None

      HadoopGeoTiffRDD.Options(
        crs = crs,
        timeTag = stringMap.getOrElse("time_tag", default.timeTag),
        timeFormat = stringMap.getOrElse("time_format", default.timeFormat),
        maxTileSize = intMap.get("max_tile_size"),
        numPartitions = intMap.get("num_partitions"),
        partitionBytes = partitionBytes,
        chunkSize = intMap.get("chunk_size")
      )
    }
  }

  object S3GeoTiffRDDOptions {
    def default = S3GeoTiffRDD.Options.DEFAULT

    def setValues(
      conf: SparkConf,
      scheme: String,
      intMap: Map[String, Int],
      stringMap: Map[String, String],
      partitionBytes: Option[Long]
    ): S3GeoTiffRDD.Options = {
      val crs: Option[CRS] =
        if (stringMap.contains("crs"))
          TileLayer.getCRS(stringMap("crs"))
        else
          None

      val getS3Client: () => S3Client = {
        val accessKey = conf.get(s"spark.hadoop.fs.${scheme}.access.key", "")
        val secretKey = conf.get(s"spark.hadoop.fs.${scheme}.secret.key", "")

        stringMap.getOrElse("s3_client", DEFAULT) match {
          case DEFAULT if accessKey.isEmpty => default.getClient
          case DEFAULT =>
            () => S3Client.builder()
              .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
              .build()
          case MOCK =>
            throw new UnsupportedOperationException("S3Client")
          case client: String => throw new Error(s"Could not find the given S3Client, $client")
        }
      }

      S3GeoTiffRDD.Options(
        crs = crs,
        timeTag = stringMap.getOrElse("time_tag", default.timeTag),
        timeFormat = stringMap.getOrElse("time_format", default.timeFormat),
        maxTileSize = intMap.get("max_tile_size"),
        numPartitions = intMap.get("num_partitions"),
        partitionBytes = partitionBytes,
        chunkSize = intMap.get("chunk_size"),
        delimiter = stringMap.get("delimiter"),
        getClient = getS3Client
      )
    }
  }

  def get(
    sc: SparkContext,
    keyType: String,
    paths: java.util.List[String],
    options: java.util.Map[String, Any],
    partitionBytes: String
  ): RasterLayer[_] = {
    val uris = paths.map{ path => new URI(path) }
    val (stringMap, intMap) = GeoTrellisUtils.convertToScalaMap(options)
    val bytes = Some(partitionBytes.toLong)
    lazy val conf = sc.getConf

    uris
      .map { uri =>
        uri.getScheme match {
          case (S3 | S3A | S3N) =>
            getS3GeoTiffRDD(
              sc,
              keyType,
              uri,
              S3GeoTiffRDDOptions.setValues(conf, uri.getScheme, intMap, stringMap, bytes)
            )
          case _ =>
              getHadoopGeoTiffRDD(
                sc,
                keyType,
                new Path(uri),
                HadoopGeoTiffRDDOptions.setValues(intMap, stringMap, bytes)
              )
        }
      }
      .reduce{ (r1, r2) =>
        keyType match {
          case PROJECTEDEXTENT =>
            ProjectedRasterLayer(r1.asInstanceOf[ProjectedRasterLayer].rdd.union(r2.asInstanceOf[ProjectedRasterLayer].rdd))
          case TEMPORALPROJECTEDEXTENT =>
            TemporalRasterLayer(r1.asInstanceOf[TemporalRasterLayer].rdd.union(r2.asInstanceOf[TemporalRasterLayer].rdd))
        }
      }
  }

  private def getHadoopGeoTiffRDD(
    sc: SparkContext,
    keyType: String,
    path: Path,
    options: HadoopGeoTiffRDD.Options
  ): RasterLayer[_] =
    keyType match {
      case PROJECTEDEXTENT =>
        ProjectedRasterLayer(HadoopGeoTiffRDD.spatialMultiband(path, options)(sc))
      case TEMPORALPROJECTEDEXTENT =>
        TemporalRasterLayer(HadoopGeoTiffRDD.temporalMultiband(path, options)(sc))
    }

  private def getS3GeoTiffRDD(
    sc: SparkContext,
    keyType: String,
    uri: URI,
    options: S3GeoTiffRDD.Options
  ): RasterLayer[_] =
    keyType match {
      case PROJECTEDEXTENT =>
        ProjectedRasterLayer(S3GeoTiffRDD.spatialMultiband(uri.getHost, uri.getPath.tail, options)(sc))
      case TEMPORALPROJECTEDEXTENT =>
        TemporalRasterLayer(S3GeoTiffRDD.temporalMultiband(uri.getHost, uri.getPath.tail, options)(sc))
    }
}
