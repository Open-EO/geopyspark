package geopyspark.geotrellis

import geotrellis.raster.histogram._
import geotrellis.raster.io._
import _root_.io.circe.syntax._
import _root_.io.circe.parser.parse
import cats.syntax.either._

object Json {

  def writeHistogram(hist: Histogram[_]): String = hist match {
    case h: FastMapHistogram =>
      h.asInstanceOf[Histogram[Int]].asJson.noSpaces
    case h: StreamingHistogram =>
      h.asInstanceOf[Histogram[Double]].asJson.noSpaces
    case _ =>
      throw new IllegalArgumentException(s"Unable to write $hist as JSON.")
  }

  def readHistogram(hist: String): Histogram[_] = {
    val json = parse(hist).valueOr(throw _)
    if (json.isObject) json.as[Histogram[Double]].valueOr(throw _)
    else if (json.isArray) json.as[Histogram[Int]].valueOr(throw _)
    else throw new AssertionError(hist)
  }
}
