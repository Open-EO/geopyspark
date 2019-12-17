package geopyspark.geotrellis

import geopyspark.util._
import geopyspark.geotrellis._
import geopyspark.geotrellis.GeoTrellisUtils._

import protos.tileMessages._
import protos.keyMessages._
import protos.tupleMessages._
import protos.featureMessages._

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.distance._
import geotrellis.raster.histogram._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.compression._
import geotrellis.raster.mapalgebra.focal.{Square, Slope}
import geotrellis.raster.mapalgebra.focal.hillshade._
import geotrellis.raster.rasterize._
import geotrellis.raster.render._
import geotrellis.raster.resample.{ResampleMethod, PointResampleMethod, Resample}
import geotrellis.raster.buffer.BufferedTile
import geotrellis.spark._
import geotrellis.spark.buffer._
import geotrellis.spark.costdistance.IterativeCostDistance
import geotrellis.spark.mapalgebra.local._
import geotrellis.spark.mapalgebra.focal._
import geotrellis.layer._
import geotrellis.layer.mask.Mask
import geotrellis.spark.mask.MaskRDD
import geotrellis.spark.pyramid._
import geotrellis.spark.rasterize._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.util._
import geotrellis.util._
import geotrellis.vector._
import geotrellis.vector.io.wkb.WKB
import geotrellis.vector.triangulation._
import geotrellis.vector.voronoi._

import _root_.io.circe.syntax._
import _root_.io.circe.parser.parse
import cats.syntax.either._
import spire.syntax.cfor._

import org.locationtech.jts.geom.Coordinate
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import java.util.ArrayList
import scala.reflect._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class SpatialTiledRasterLayer(
  val zoomLevel: Option[Int],
  val rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]
) extends TiledRasterLayer[SpatialKey] {

  def lookup(
    col: Int,
    row: Int
  ): java.util.ArrayList[Array[Byte]] = {
    val tiles = rdd.lookup(SpatialKey(col, row))
    PythonTranslator.toPython[MultibandTile, ProtoMultibandTile](tiles)
  }

  def reproject(
    targetCRS: String,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): SpatialTiledRasterLayer = {
    val partitioner = TileLayer.getPartitioner(partitionStrategy, rdd.getNumPartitions)

    val crs = TileLayer.getCRS(targetCRS).get
    val targetLayout = FloatingLayoutScheme(rdd.metadata.layout.tileCols, rdd.metadata.layout.tileRows)
    val (zoom, reprojected) = rdd.reproject(crs, targetLayout, resampleMethod, partitioner)
    new SpatialTiledRasterLayer(Some(zoom), reprojected)
  }

  def reproject(
    targetCRS: String,
    layoutType: LayoutType,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): SpatialTiledRasterLayer = {
    val partitioner = TileLayer.getPartitioner(partitionStrategy, rdd.getNumPartitions)
    val crs = TileLayer.getCRS(targetCRS).get

    layoutType match {
      case GlobalLayout(tileSize, null, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (zoom, reprojected) = rdd.reproject(crs, scheme, resampleMethod, partitioner)
        SpatialTiledRasterLayer(Some(zoom), reprojected)

      case GlobalLayout(tileSize, zoom, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (_, reprojected) = TileRDDReproject(rdd, crs, Right(scheme.levelForZoom(zoom).layout), resampleMethod, partitioner)
        SpatialTiledRasterLayer(zoom, reprojected)

      case LocalLayout(tileCols, tileRows) =>
        val (_, reprojected) = rdd.reproject(crs, FloatingLayoutScheme(tileCols, tileRows), resampleMethod, partitioner)
        SpatialTiledRasterLayer(None, reprojected)
    }
  }

  def reproject(
    targetCRS: String,
    layoutDefinition: LayoutDefinition,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): SpatialTiledRasterLayer = {
    val partitioner = TileLayer.getPartitioner(partitionStrategy, rdd.getNumPartitions)

    val (zoom, reprojected) = TileRDDReproject(rdd, TileLayer.getCRS(targetCRS).get, Right(layoutDefinition), resampleMethod, partitioner)
    SpatialTiledRasterLayer(Some(zoom), reprojected)
  }

  def tileToLayout(
    layoutDefinition: LayoutDefinition,
    zoom: Option[Int],
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpatialKey] = {
    val baseTransform = rdd.metadata.layout.mapTransform
    val targetTransform = layoutDefinition.mapTransform
    val crs = rdd.metadata.crs

    val projectedRDD = rdd.map { case(k, v) => (ProjectedExtent(baseTransform(k), crs), v) }

    val retiledLayerMetadata = rdd.metadata.copy(
      layout = layoutDefinition,
      bounds = KeyBounds(targetTransform(rdd.metadata.extent))
    )

    val options = getTilerOptions(resampleMethod, partitionStrategy)
    val tileLayer =
      MultibandTileLayerRDD(projectedRDD.tileToLayout(retiledLayerMetadata, options), retiledLayerMetadata)

    SpatialTiledRasterLayer(zoom, tileLayer)
  }

  def pyramid(resampleMethod: ResampleMethod, partitionStrategy: PartitionStrategy): Array[TiledRasterLayer[SpatialKey]] = {
    require(! rdd.metadata.bounds.isEmpty, "Can not pyramid an empty RDD")

    val partitioner =
      partitionStrategy match {
        case ps: PartitionStrategy => ps.producePartitioner(rdd.getNumPartitions)
        case null => None
      }

    val (baseZoom, scheme) =
      zoomLevel match {
        case Some(zoom) =>
          zoom -> ZoomedLayoutScheme(rdd.metadata.crs, rdd.metadata.tileRows)

        case None =>
          val zoom = LocalLayoutScheme.inferLayoutLevel(rdd.metadata.layout)
          zoom -> new LocalLayoutScheme
      }

    Pyramid.levelStream(
      rdd, scheme, baseZoom, 0,
      Pyramid.Options(resampleMethod=resampleMethod, partitioner=partitioner)
    ).map{ x =>
      SpatialTiledRasterLayer(Some(x._1), x._2)
    }.toArray
  }

  def focal(
    operation: String,
    neighborhood: String,
    param1: Double,
    param2: Double,
    param3: Double,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpatialKey] = {
    val singleTileLayerRDD: TileLayerRDD[SpatialKey] = TileLayerRDD(
      rdd.mapValues({ v => v.band(0) }),
      rdd.metadata
    )

    val _neighborhood =
      if (operation == Constants.SLOPE || operation == Constants.ASPECT)
        getNeighborhood(neighborhood, 1.0, 0.0, 0.0)
      else
        getNeighborhood(neighborhood, param1, param2, param3)

    val cellSize = rdd.metadata.layout.cellSize
    val op: ((Tile, Option[GridBounds[Int]]) => Tile) = getOperation(operation, _neighborhood, cellSize, param1)

    val result: TileLayerRDD[SpatialKey] =
      partitionStrategy match {
        case ps: PartitionStrategy =>
          FocalOperation(singleTileLayerRDD, _neighborhood, ps.producePartitioner(rdd.getNumPartitions))(op)
        case null =>
          FocalOperation(singleTileLayerRDD, _neighborhood, None)(op)
      }

    val multibandRDD: MultibandTileLayerRDD[SpatialKey] =
      MultibandTileLayerRDD(result.mapValues{ x => MultibandTile(x) }, result.metadata)

    SpatialTiledRasterLayer(None, multibandRDD)
  }

  def slope(zFactorCalculator: ZFactorCalculator): SpatialTiledRasterLayer = {
    val mt = rdd.metadata.mapTransform
    val cellSize = rdd.metadata.cellSize

    SpatialTiledRasterLayer(
      zoomLevel,
      rdd.withContext { rdd =>
        rdd.bufferTiles(bufferSize = 1).mapPartitions[(SpatialKey, MultibandTile)](
        { iter =>
          iter.map { case (key, BufferedTile(tile, bounds)) =>
            val zfactor = zFactorCalculator.deriveZFactor(mt.keyToExtent(key))
            val slopeTile = Slope(tile.bands(0), Square(1), Some(bounds), cellSize, zfactor).interpretAs(FloatConstantNoDataCellType)

            key -> MultibandTile(slopeTile)
          }
        }, preservesPartitioning = true
        )
      }.mapContext(_.copy(cellType = FloatConstantNoDataCellType))
    )
  }

  def hillshade(
    azimuth: Double,
    altitude: Double,
    zFactorCalculator: ZFactorCalculator,
    band: Int
  ): SpatialTiledRasterLayer = {
    val mt = rdd.metadata.mapTransform
    val cellSize = rdd.metadata.cellSize
    val neighborhood = Square(1)
    val gridBounds = rdd.metadata.tileBounds
    val partitioner = rdd.partitioner

    SpatialTiledRasterLayer(
      zoomLevel,
      rdd.withContext { rdd =>
        rdd.bufferTiles(neighborhood.extent, gridBounds, partitioner).mapPartitions[(SpatialKey, MultibandTile)](
        { iter =>
          iter.map { case (key, BufferedTile(tile, bounds)) =>
            val zfactor = zFactorCalculator.deriveZFactor(mt.keyToExtent(key))
            val hillshadeTile = Hillshade(tile.bands(band), neighborhood, Some(bounds), cellSize, azimuth, altitude, zfactor)

            key -> MultibandTile(hillshadeTile)
          }
        }, preservesPartitioning = true
        )
      }.mapContext(_.copy(cellType = ShortConstantNoDataCellType))
    )
  }

  def mask(geometries: Seq[MultiPolygon]): TiledRasterLayer[SpatialKey] =
    SpatialTiledRasterLayer(zoomLevel, MaskRDD(rdd, geometries, Mask.Options.DEFAULT))

  def mask(
    groupedRDD: RDD[(SpatialKey, Iterable[Geometry])],
    options: Rasterizer.Options
  ): TiledRasterLayer[SpatialKey] = {
    val mapTrans = rdd.metadata.layout.mapTransform

    val joinedRDD: RDD[(SpatialKey, (MultibandTile, Iterable[Geometry]))] =
      groupedRDD.partitioner match {
        case Some(p) =>
          rdd.join(groupedRDD, p)
        case None =>
          rdd.join(groupedRDD)
      }

    val maskedRDD: RDD[(SpatialKey, MultibandTile)] =
      joinedRDD.mapPartitions ({ partition =>
        partition.map { case (k, (v, geoms)) =>
          (k, v.mask(mapTrans(k), geoms, options))
        }
      }, preservesPartitioning = true)

    SpatialTiledRasterLayer(zoomLevel, ContextRDD(maskedRDD, rdd.metadata))
  }

  def getCellValueCounts(areaOfInterest: Array[Byte], targetBand: Int): String = {
    val acc = new CountingAccumulator()
    rdd.sparkContext.register(acc)

    val geom =
      areaOfInterest match {
        case arr: Array[Byte] => Some(WKB.read(arr))
        case _ => None
      }

    val md = rdd.metadata

    val singleBandLayer = rdd.withContext { _.mapValues { _.band(targetBand) } }

    singleBandLayer.foreach { case (k, v) =>
      def extent = md.mapTransform(k.getComponent[SpatialKey])
      val rasterExtent = RasterExtent(extent, md.tileCols, md.tileRows)

      Rasterizer.foreachCellByGeometry(geom.getOrElse(extent), rasterExtent) { case (col, row) =>
        acc.add(v.get(col, row).toInt)
      }
    }

    acc
      .value
      .groupBy(_._1)
      .map { case (k, vs) => k.toString -> vs.map { case (_, v) => v }.sum.toString }
      .asJson
      .noSpaces
  }

  def stitch: Array[Byte] =
    PythonTranslator.toPython[MultibandTile, ProtoMultibandTile](ContextRDD(rdd, rdd.metadata).stitch.tile)

  def saveStitched(path: String): Unit =
    saveStitched(path, None, None)

  def saveStitched(path: String, cropBounds: java.util.Map[String, Double]): Unit =
    saveStitched(path, Some(cropBounds), None)

  def saveStitched(
    path: String,
    cropBounds: java.util.Map[String, Double],
    cropDimensions: ArrayList[Int]
  ): Unit =
    saveStitched(path, Some(cropBounds), Some(cropDimensions))

  def saveStitched(
    path: String,
    cropBounds: Option[java.util.Map[String, Double]],
    cropDimensions: Option[ArrayList[Int]]
  ): Unit = {
    val contextRDD = ContextRDD(rdd, rdd.metadata)

    val stitched: Raster[MultibandTile] = contextRDD.stitch()

    val adjusted = {
      val cropped =
        cropBounds match {
          case Some(extent) => stitched.crop(extent.toExtent)
          case None => stitched
        }

      val resampled =
        cropDimensions.map(_.asScala.toArray) match {
          case Some(dimensions) =>
            cropped.resample(dimensions(0), dimensions(1))
          case None =>
            cropped
        }

      resampled
    }

    GeoTiff(adjusted, contextRDD.metadata.crs).write(path)
  }

  def costDistance(
    sc: SparkContext,
    geometries: Seq[Geometry],
    maxDistance: Double
  ): TiledRasterLayer[SpatialKey] = {
    val singleTileLayer = TileLayerRDD(
      rdd.mapValues({ v => v.band(0) }),
      rdd.metadata
    )

    implicit def conversion(k: SpaceTimeKey): SpatialKey =
      k.spatialKey

    implicit val _sc = sc

    val result: TileLayerRDD[SpatialKey] =
      IterativeCostDistance(singleTileLayer, geometries, maxDistance)

    val multibandRDD: MultibandTileLayerRDD[SpatialKey] =
      MultibandTileLayerRDD(result.mapValues{ x => MultibandTile(x) }, result.metadata)

    SpatialTiledRasterLayer(None, multibandRDD)
  }

  def reclassify(reclassifiedRDD: RDD[(SpatialKey, MultibandTile)]): TiledRasterLayer[SpatialKey] =
    SpatialTiledRasterLayer(zoomLevel, MultibandTileLayerRDD(reclassifiedRDD, rdd.metadata))

  def reclassifyDouble(reclassifiedRDD: RDD[(SpatialKey, MultibandTile)]): TiledRasterLayer[SpatialKey] =
    SpatialTiledRasterLayer(zoomLevel, MultibandTileLayerRDD(reclassifiedRDD, rdd.metadata))

  def withRDD(result: RDD[(SpatialKey, MultibandTile)]): TiledRasterLayer[SpatialKey] =
    SpatialTiledRasterLayer(zoomLevel, MultibandTileLayerRDD(result, rdd.metadata))

  def withContextRDD(result: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]): TiledRasterLayer[SpatialKey] =
    SpatialTiledRasterLayer(zoomLevel, result)

  def toInt(converted: RDD[(SpatialKey, MultibandTile)]): TiledRasterLayer[SpatialKey] =
    SpatialTiledRasterLayer(zoomLevel, MultibandTileLayerRDD(converted, rdd.metadata))

  def toDouble(converted: RDD[(SpatialKey, MultibandTile)]): TiledRasterLayer[SpatialKey] =
    SpatialTiledRasterLayer(zoomLevel, MultibandTileLayerRDD(converted, rdd.metadata))

  def toProtoRDD(): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(SpatialKey, MultibandTile), ProtoTuple](rdd)

  def toPngRDD(pngRDD: RDD[(SpatialKey, Array[Byte])]): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(SpatialKey, Array[Byte]), ProtoTuple](pngRDD)

  def toGeoTiffRDD(
    tags: Tags,
    resampleMethod: ResampleMethod,
    decimations: List[Int],
    geoTiffOptions: GeoTiffOptions
  ): JavaRDD[Array[Byte]] = {
    val mapTransform =
      MapKeyTransform(
        rdd.metadata.layout.extent,
        rdd.metadata.layout.layoutCols,
        rdd.metadata.layout.layoutRows
      )

    val crs = rdd.metadata.crs

    val geotiffRDD =
      rdd.map { case (k, v) =>
        val transKey = ProjectedExtent(mapTransform(k), crs)

        val geoTiff =
          MultibandGeoTiff(v, transKey.extent, transKey.crs, tags, geoTiffOptions)
            .withOverviews(resampleMethod, decimations)

        (k, geoTiff.toByteArray)
      }

    PythonTranslator.toPython[(SpatialKey, Array[Byte]), ProtoTuple](geotiffRDD)
  }

  def collectKeys(): java.util.ArrayList[Array[Byte]] =
    PythonTranslator.toPython[SpatialKey, ProtoSpatialKey](rdd.keys.collect)

  def getPointValues(
    points: java.util.Map[Long, Array[Byte]],
    resampleMethod: PointResampleMethod
  ): java.util.Map[Long, Array[Double]] = {
    val mapTrans = rdd.metadata.layout.mapTransform

    val idedKeys: Map[Long, Point] =
      points
        .asScala
        .mapValues { WKB.read(_).asInstanceOf[Point] }
        .toMap

    val pointKeys =
      idedKeys
        .foldLeft(Map[SpatialKey, Array[(Long, Point)]]()) {
          case (acc, elem) =>
            val pointKey = mapTrans(elem._2)

            acc.get(pointKey) match {
              case Some(arr) => acc + (pointKey -> (elem +: arr))
              case None => acc + (pointKey -> Array(elem))
            }
        }

    val matchedKeys =
      resampleMethod match {
        case r: PointResampleMethod => _getPointValues(pointKeys, mapTrans, r)
        case _ => _getPointValues(pointKeys, mapTrans)
      }

    val remainingKeys = idedKeys.keySet diff matchedKeys.keySet

    if (remainingKeys.isEmpty)
      matchedKeys.asJava
    else
      remainingKeys.foldLeft(matchedKeys){ case (acc, elem) =>
        acc + (elem -> null)
      }.asJava
  }

  def _getPointValues(
    pointKeys: Map[SpatialKey, Array[(Long, Point)]],
    mapTrans: MapKeyTransform,
    resampleMethod: PointResampleMethod
  ): Map[Long, Array[Double]] = {
    val resamplePoint = (tile: Tile, extent: Extent, point: Point) => {
      Resample(resampleMethod, tile, extent).resampleDouble(point)
    }

    rdd.flatMap { case (k, v) =>
      pointKeys.get(k) match {
        case Some(arr) =>
          val keyExtent = mapTrans(k)
          val rasterExtent = RasterExtent(keyExtent, v.cols, v.rows)

          arr.map { case (id, point) =>
            (id, v.bands.map { resamplePoint(_, keyExtent, point) } toArray)
          }
        case None => Seq()
      }
    }.collect().toMap
  }

  def _getPointValues(
    pointKeys: Map[SpatialKey, Array[(Long, Point)]],
    mapTrans: MapKeyTransform
  ): Map[Long, Array[Double]] =
    rdd.flatMap { case (k, v) =>
      pointKeys.get(k) match {
        case Some(arr) =>
          val keyExtent = mapTrans(k)
          val rasterExtent = RasterExtent(keyExtent, v.cols, v.rows)

          arr.map { case (id, point) =>
            val (gridCol, gridRow) = rasterExtent.mapToGrid(point)

            val values = Array.ofDim[Double](v.bandCount)

            cfor(0)(_ < v.bandCount, _ + 1){ index =>
              values(index) = v.band(index).getDouble(gridCol, gridRow)
            }

            (id, values)
          }
        case None => Seq()
      }
    }.collect().toMap
}


object SpatialTiledRasterLayer {
  def fromProtoEncodedRDD(
    javaRDD: JavaRDD[Array[Byte]],
    metadata: String
  ): SpatialTiledRasterLayer = {
    val md = parse(metadata).valueOr(throw _).as[TileLayerMetadata[SpatialKey]].valueOr(throw _)
    val tileLayer = MultibandTileLayerRDD(
      PythonTranslator.fromPython[(SpatialKey, MultibandTile), ProtoTuple](javaRDD, ProtoTuple.parseFrom), md)

    SpatialTiledRasterLayer(None, tileLayer)
  }

  def fromProtoEncodedRDD(
    javaRDD: JavaRDD[Array[Byte]],
    zoomLevel: Int,
    metadata: String
  ): SpatialTiledRasterLayer = {
    val md = parse(metadata).valueOr(throw _).as[TileLayerMetadata[SpatialKey]].valueOr(throw _)
    val tileLayer = MultibandTileLayerRDD(
      PythonTranslator.fromPython[(SpatialKey, MultibandTile), ProtoTuple](javaRDD, ProtoTuple.parseFrom), md)

    SpatialTiledRasterLayer(Some(zoomLevel), tileLayer)
  }

  def apply(
    zoomLevel: Integer,
    rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]
  ): SpatialTiledRasterLayer =
    zoomLevel match {
      case i: Integer => apply(Some(i.toInt), rdd)
      case null => apply(None, rdd)
    }

  def apply(
    zoomLevel: Option[Int],
    rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]
  ): SpatialTiledRasterLayer =
    new SpatialTiledRasterLayer(zoomLevel, rdd)

  def rasterizeGeometry(
    geomWKB: RDD[Array[Byte]],
    geomCRS: String,
    requestedZoom: Int,
    fillValue: Double,
    cellType: String,
    options: Rasterizer.Options,
    partitionStrategy: PartitionStrategy
  ): SpatialTiledRasterLayer = {
    val geomRDD = geomWKB.map { WKB.read }
    val fullEnvelope = geomRDD.map(_.extent).reduce(_ combine _)

    rasterizeGeometry(
      geomRDD,
      geomCRS,
      requestedZoom,
      fillValue,
      cellType,
      fullEnvelope,
      options,
      partitionStrategy)
  }

  def rasterizeGeometry(
    sc: SparkContext,
    geomWKB: ArrayList[Array[Byte]],
    geomCRS: String,
    requestedZoom: Int,
    fillValue: Double,
    cellType: String,
    options: Rasterizer.Options,
    partitionStrategy: PartitionStrategy
  ): SpatialTiledRasterLayer = {
    val geoms = geomWKB.asScala.map(WKB.read)
    val fullEnvelope = geoms.map(_.extent).reduce(_ combine _)
    val geomRDD = sc.parallelize(geoms)

    rasterizeGeometry(
      geomRDD,
      geomCRS,
      requestedZoom,
      fillValue,
      cellType,
      fullEnvelope,
      options,
      partitionStrategy)
  }

  def rasterizeGeometry(
    geomRDD: RDD[Geometry],
    geomCRS: String,
    requestedZoom: Int,
    fillValue: Double,
    requestedCellType: String,
    extent: Extent,
    options: Rasterizer.Options,
    partitionStrategy: PartitionStrategy
  ): SpatialTiledRasterLayer = {
    import geotrellis.raster.rasterize.Rasterizer.Options

    val cellType = CellType.fromName(requestedCellType)
    val srcCRS = TileLayer.getCRS(geomCRS).get
    val LayoutLevel(z, ld) = ZoomedLayoutScheme(srcCRS).levelForZoom(requestedZoom)
    val maptrans = ld.mapTransform
    val gb @ GridBounds(cmin, rmin, cmax, rmax) = maptrans(extent)

    val partitioner =
      partitionStrategy match {
        case ps: PartitionStrategy => ps.producePartitioner(math.max(gb.size.toInt / 512, 1))
        case null => None
      }

    val tiles = RasterizeRDD.fromGeometry(
      geoms = geomRDD,
      layout = ld,
      cellType = cellType,
      value = fillValue,
      options = Option(options).getOrElse(Options.DEFAULT),
      partitioner = partitioner)

    val metadata = TileLayerMetadata(cellType, ld, maptrans(gb), srcCRS, KeyBounds(gb))

    SpatialTiledRasterLayer(Some(requestedZoom),
      MultibandTileLayerRDD(tiles.mapValues(MultibandTile(_)), metadata))
  }

  def rasterizeFeaturesWithZIndex(
    featureRDD: RDD[Array[Byte]],
    geomCRS: String,
    requestedZoom: Int,
    requestedCellType: String,
    options: Rasterizer.Options,
    zIndexCellType: String,
    partitionStrategy: PartitionStrategy
  ): SpatialTiledRasterLayer = {
    import geotrellis.raster.rasterize.Rasterizer.Options

    val scalaRDD =
      PythonTranslator.fromPython[Feature[Geometry, CellValue], ProtoFeatureCellValue](featureRDD, ProtoFeatureCellValue.parseFrom)

    val fullEnvelope = scalaRDD.map(_.geom.extent).reduce(_ combine _)

    val cellType = CellType.fromName(requestedCellType)
    val zCellType = CellType.fromName(zIndexCellType)

    val srcCRS = TileLayer.getCRS(geomCRS).get
    val LayoutLevel(z, ld) = ZoomedLayoutScheme(srcCRS).levelForZoom(requestedZoom)
    val maptrans = ld.mapTransform
    val gb @ GridBounds(cmin, rmin, cmax, rmax) = maptrans(fullEnvelope)

    val partitioner =
      partitionStrategy match {
        case ps: PartitionStrategy => ps.producePartitioner(math.max(gb.size.toInt / 512, 1))
        case null => None
      }

    val tiles =
      RasterizeRDD.fromFeatureWithZIndex(
        features = scalaRDD,
        cellType = cellType,
        layout = ld,
        options = Option(options).getOrElse(Options.DEFAULT),
        partitioner = partitioner,
        zIndexCellType = zCellType
      )

    val metadata = TileLayerMetadata(cellType, ld, maptrans(gb), srcCRS, KeyBounds(gb))

    SpatialTiledRasterLayer(Some(requestedZoom),
      MultibandTileLayerRDD(tiles.mapValues(MultibandTile(_)), metadata))
  }

  def euclideanDistance(sc: SparkContext, geomWKB: Array[Byte], geomCRSStr: String, cellTypeString: String, requestedZoom: Int): TiledRasterLayer[SpatialKey]= {
    val cellType = CellType.fromName(cellTypeString)
    val geom = WKB.read(geomWKB)
    val srcCRS = TileLayer.getCRS(geomCRSStr).get
    val LayoutLevel(z, ld) = ZoomedLayoutScheme(srcCRS).levelForZoom(requestedZoom)
    val maptrans = ld.mapTransform
    val gb @ GridBounds(cmin, rmin, cmax, rmax) = maptrans(geom.extent)

    val keys = for (r <- rmin to rmax; c <- cmin to cmax) yield SpatialKey(c, r)

    val pts =
      if (geom.isInstanceOf[MultiPoint]) {
        geom.asInstanceOf[MultiPoint].points.map(_.getCoordinate)
      } else {
        val coords = collection.mutable.ListBuffer.empty[Coordinate]

        def createPoints(sk: SpatialKey) = {
          val ex = maptrans(sk)
          val re = RasterExtent(ex, ld.tileCols, ld.tileRows)

          def rasterizeToPoints(px: Int, py: Int): Unit = {
            val (x, y) = re.gridToMap(px, py)
            val coord = new Coordinate(x, y)
            coords += coord
          }

          Rasterizer.foreachCellByGeometry(geom, re)(rasterizeToPoints)
        }

        keys.foreach(createPoints)
        coords.toArray
      }

    val dt = KryoWrapper(DelaunayTriangulation(pts))
    val skRDD = sc.parallelize(keys)
    val mbtileRDD: RDD[(SpatialKey, MultibandTile)] = skRDD.mapPartitions({ skiter => skiter.map { sk =>
      val ex = maptrans(sk)
      val re = RasterExtent(ex, ld.tileCols, ld.tileRows)
      val tile = ArrayTile.empty(cellType, re.cols, re.rows)
      val vd = new VoronoiDiagram(dt.value, ex)
      vd.voronoiCellsWithPoints.foreach(EuclideanDistanceTile.rasterizeDistanceCell(re, tile)(_))
      (sk, MultibandTile(tile))
    } }, preservesPartitioning=true)

    val metadata = TileLayerMetadata(cellType, ld, maptrans(gb), srcCRS, KeyBounds(gb))

    SpatialTiledRasterLayer(Some(z), MultibandTileLayerRDD(mbtileRDD, metadata))
  }

  def unionLayers(sc: SparkContext, layers: ArrayList[SpatialTiledRasterLayer]): SpatialTiledRasterLayer = {
    val scalaLayers = layers.asScala

    val result = sc.union(scalaLayers.map(_.rdd))

    val firstLayer = scalaLayers.head
    val zoomLevel = firstLayer.zoomLevel

    var unionedMetadata = firstLayer.rdd.metadata

    for (x <- 1 until scalaLayers.size) {
      val otherMetadata = scalaLayers(x).rdd.metadata
      unionedMetadata = unionedMetadata.combine(otherMetadata)
    }

    SpatialTiledRasterLayer(zoomLevel, ContextRDD(result, unionedMetadata))
  }

  def combineBands(sc: SparkContext, layers: ArrayList[SpatialTiledRasterLayer]): SpatialTiledRasterLayer = {
    val baseLayer: SpatialTiledRasterLayer = layers.get(0)
    val result: RDD[(SpatialKey, MultibandTile)] =
      TileLayer.combineBands[SpatialKey, SpatialTiledRasterLayer](sc, layers)

    SpatialTiledRasterLayer(baseLayer.zoomLevel, ContextRDD(result, baseLayer.rdd.metadata))
  }
}
