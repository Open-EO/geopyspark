package geopyspark.geotools.shapefile

import geopyspark.geotools._
import geopyspark.util._

import protos.simpleFeatureMessages._

import geotrellis.vector._
import geotrellis.store.s3._
import software.amazon.awssdk.services.s3.S3Client
import geotrellis.geotools._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD

import org.opengis.feature.simple._

import java.net.URI

import scala.collection.JavaConverters._


object ShapefileRDD {
  def get(
    sc: SparkContext,
    paths: java.util.ArrayList[String],
    extensions: java.util.ArrayList[String],
    numPartitions: Int,
    s3Client: String
  ): JavaRDD[Array[Byte]] = {
    val uris: Array[URI] = paths.asScala.map { path => new URI(path) }.toArray

    val simpleFeaturesRDD: RDD[SimpleFeature] =
      uris.head.getScheme match {
        case "s3" =>
          val client =
            s3Client match {
              case null => S3Client.create()
              case s: String =>
                s match {
                  case "default" => S3Client.create()
                  case "mock" => throw new UnsupportedOperationException("S3Client")
                  case _ => throw new Exception(s"Unkown S3Client specified, ${s}")
                }
            }
          S3ShapefileRDD.createSimpleFeaturesRDD(sc, uris, extensions.asScala, client, numPartitions)
        case _ =>
          HadoopShapefileRDD.createSimpleFeaturesRDD(sc, uris, extensions.asScala, numPartitions)
      }

    val featuresRDD: RDD[Feature[Geometry, Map[String, AnyRef]]] =
      simpleFeaturesRDD.map { SimpleFeatureToFeature(_) }

    PythonTranslator.toPython[Feature[Geometry, Map[String, AnyRef]], ProtoSimpleFeature](featuresRDD)
  }
}
