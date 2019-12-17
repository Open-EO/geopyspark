package geopyspark.geotrellis

import geopyspark.util._

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.spark._
import geotrellis.layer._
import geotrellis.spark.reproject._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark._
import org.apache.spark.rdd.RDD

import protos.tupleMessages.ProtoTuple
import protos.extentMessages.ProtoTemporalProjectedExtent

import scala.util.{Either, Left, Right}
import scala.collection.JavaConverters._

import java.util.ArrayList
import java.time.{ZonedDateTime, ZoneId}

import _root_.io.circe.syntax._
import _root_.io.circe.parser.parse
import cats.syntax.either._

class TemporalRasterLayer(val rdd: RDD[(TemporalProjectedExtent, MultibandTile)]) extends RasterLayer[TemporalProjectedExtent] {

  def collectMetadata(layoutType: LayoutType): String = {
    val sms = RasterSummary.collect[TemporalProjectedExtent, SpaceTimeKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")

    sms.head.toTileLayerMetadata(layoutType)._1.asJson.noSpaces
  }

  def collectMetadata(layoutDefinition: LayoutDefinition): String = {
    val sms = RasterSummary.collect[TemporalProjectedExtent, SpaceTimeKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")
    val sm = sms.head

    TileLayerMetadata[SpaceTimeKey](
      sm.cellType,
      layoutDefinition,
      sm.extent,
      sm.crs,
      sm.bounds.setSpatialBounds(layoutDefinition.mapTransform(sm.extent))
    ).asJson.noSpaces
  }

  def tileToLayout(
    layerMetadata: String,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpaceTimeKey] = {
    val md = parse(layerMetadata).valueOr(throw _).as[TileLayerMetadata[SpaceTimeKey]].valueOr(throw _)
    val options = getTilerOptions(resampleMethod, partitionStrategy)

    new TemporalTiledRasterLayer(None, MultibandTileLayerRDD(rdd.tileToLayout(md, options), md))
  }

  def tileToLayout(
    layoutDefinition: LayoutDefinition,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpaceTimeKey] = {
    val sms = RasterSummary.collect[TemporalProjectedExtent, SpaceTimeKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")

    val sm = sms.head
    val metadata = TileLayerMetadata[SpaceTimeKey](
      sm.cellType,
      layoutDefinition,
      sm.extent,
      sm.crs,
      sm.bounds.setSpatialBounds(layoutDefinition.mapTransform(sm.extent))
    )

    val options = getTilerOptions(resampleMethod, partitionStrategy)

    TemporalTiledRasterLayer(None, MultibandTileLayerRDD(rdd.tileToLayout(metadata, options), metadata))
  }

  def tileToLayout(
    layoutType: LayoutType,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpaceTimeKey] ={
    val sms = RasterSummary.collect[TemporalProjectedExtent, SpaceTimeKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")

    val sm = sms.head
    val (metadata, zoom) = sm.toTileLayerMetadata(layoutType)
    val options = getTilerOptions(resampleMethod, partitionStrategy)
    val tiled = rdd.tileToLayout(metadata, options)

    new TemporalTiledRasterLayer(zoom, MultibandTileLayerRDD(tiled, metadata))
  }

  def reproject(targetCRS: String, resampleMethod: ResampleMethod): TemporalRasterLayer = {
    val crs = TileLayer.getCRS(targetCRS).get
    new TemporalRasterLayer(rdd.reproject(crs, resampleMethod))
  }

  def reproject(
    targetCRS: String,
    layoutType: LayoutType,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpaceTimeKey] = {
    val partitioner = TileLayer.getPartitioner(partitionStrategy, rdd.getNumPartitions)

    val crs = TileLayer.getCRS(targetCRS).get
    val tiled = tileToLayout(LocalLayout(256), resampleMethod, partitionStrategy).rdd

    layoutType match {
      case GlobalLayout(tileSize, null, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (zoom, reprojected) = tiled.reproject(crs, scheme, resampleMethod, partitioner)
        new TemporalTiledRasterLayer(Some(zoom), reprojected)

      case GlobalLayout(tileSize, zoom, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (_, reprojected) = TileRDDReproject(tiled, crs, Right(scheme.levelForZoom(zoom).layout), resampleMethod, partitioner)
        new TemporalTiledRasterLayer(Some(zoom), reprojected)

      case LocalLayout(tileCols, tileRows) =>
        val (_, reprojected) = tiled.reproject(crs, FloatingLayoutScheme(tileCols, tileRows), resampleMethod, partitioner)
        new TemporalTiledRasterLayer(None, reprojected)
    }
  }

  def reproject(
    targetCRS: String,
    layoutDefinition: LayoutDefinition,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpaceTimeKey] = {
    val partitioner = TileLayer.getPartitioner(partitionStrategy, rdd.getNumPartitions)
    val tiled = tileToLayout(layoutDefinition, resampleMethod, partitionStrategy).rdd
    val (zoom, reprojected) =
      TileRDDReproject(tiled, TileLayer.getCRS(targetCRS).get, Right(layoutDefinition), resampleMethod, partitioner)

    TemporalTiledRasterLayer(Some(zoom), reprojected)
  }

  def reclassify(reclassifiedRDD: RDD[(TemporalProjectedExtent, MultibandTile)]): RasterLayer[TemporalProjectedExtent] =
    TemporalRasterLayer(reclassifiedRDD)

  def reclassifyDouble(reclassifiedRDD: RDD[(TemporalProjectedExtent, MultibandTile)]): RasterLayer[TemporalProjectedExtent] =
    TemporalRasterLayer(reclassifiedRDD)

  def withRDD(result: RDD[(TemporalProjectedExtent, MultibandTile)]): RasterLayer[TemporalProjectedExtent] =
    TemporalRasterLayer(result)

  def toProtoRDD(): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(TemporalProjectedExtent, MultibandTile), ProtoTuple](rdd)

  def toPngRDD(pngRDD: RDD[(TemporalProjectedExtent, Array[Byte])]): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(TemporalProjectedExtent, Array[Byte]), ProtoTuple](pngRDD)

  def toGeoTiffRDD(
    tags: Tags,
    resampleMethod: ResampleMethod,
    decimations: List[Int],
    geoTiffOptions: GeoTiffOptions
  ): JavaRDD[Array[Byte]] = {
    val geotiffRDD =
      rdd.map { case (k, v) =>
        val geoTiff =
          MultibandGeoTiff(v, k.extent, k.crs, tags, geoTiffOptions)
            .withOverviews(resampleMethod, decimations)

        (k, geoTiff.toByteArray)
      }

    PythonTranslator.toPython[(TemporalProjectedExtent, Array[Byte]), ProtoTuple](geotiffRDD)
  }

  def toSpatialLayer(instant: Long): ProjectedRasterLayer = {
    val spatialRDD =
      rdd
        .filter { case (key, _) => key.instant == instant }
        .map { x => (x._1.projectedExtent, x._2) }

    ProjectedRasterLayer(spatialRDD)
  }

  def toSpatialLayer(): ProjectedRasterLayer =
    ProjectedRasterLayer(rdd.map { x => (x._1.projectedExtent, x._2) })

  def collectKeys(): java.util.ArrayList[Array[Byte]] =
    PythonTranslator.toPython[TemporalProjectedExtent, ProtoTemporalProjectedExtent](rdd.keys.collect)

  def filterByTimes(
    times: java.util.ArrayList[String]
  ): TemporalRasterLayer = {
    val timeBoundaries: Array[(Long, Long)] =
      times
        .asScala
        .grouped(2)
        .map { list =>
          list match {
            case scala.collection.mutable.Buffer(a, b) =>
              (ZonedDateTime.parse(a).toInstant.toEpochMilli, ZonedDateTime.parse(b).toInstant.toEpochMilli)
            case scala.collection.mutable.Buffer(a) =>
              (ZonedDateTime.parse(a).toInstant.toEpochMilli, ZonedDateTime.parse(a).toInstant.toEpochMilli)
          }
        }.toArray

      val inRange = (tpe: TemporalProjectedExtent, range: (Long, Long)) =>
        range._1 <= tpe.instant && tpe.instant <= range._2

    val filteredRDD =
      rdd.filter { case (key, _) => timeBoundaries.filter(inRange(key, _)).size != 0 }

    TemporalRasterLayer(filteredRDD)
  }
}


object TemporalRasterLayer {
  def fromProtoEncodedRDD(javaRDD: JavaRDD[Array[Byte]]): TemporalRasterLayer =
    TemporalRasterLayer(
      PythonTranslator.fromPython[
        (TemporalProjectedExtent, MultibandTile), ProtoTuple
      ](javaRDD, ProtoTuple.parseFrom))

  def apply(rdd: RDD[(TemporalProjectedExtent, MultibandTile)]): TemporalRasterLayer =
    new TemporalRasterLayer(rdd)

  def unionLayers(
    sc: SparkContext,
    layers: ArrayList[TemporalRasterLayer]
  ): TemporalRasterLayer =
    TemporalRasterLayer(sc.union(layers.asScala.map(_.rdd)))

  def combineBands(sc: SparkContext, layers: ArrayList[TemporalRasterLayer]): TemporalRasterLayer =
    TemporalRasterLayer(TileLayer.combineBands[TemporalProjectedExtent, TemporalRasterLayer](sc, layers))
}
