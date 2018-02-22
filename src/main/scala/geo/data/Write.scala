package geo.data

import java.io._
import geo.elements._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

object Write {

  case class Empty()
  case class Geometry(geometry: Any,`type`: String = "Feature", properties: Any = Empty())
  case class PointGeoJSON(coordinates: List[Double],`type`: String = "Point")
  case class WayGeoJSON(coordinates: List[List[Double]],`type`: String = "LineString")

  implicit val formats: DefaultFormats = DefaultFormats

  def resultsToJSON (pw: PrintWriter, data: List[(Point, Point, List[Way])]): Unit = {

    pw.write("{\"type\": \"FeatureCollection\" ,\"features\":[")

    writeFeatures(pw, data)

    pw.write("]}")

  }

  def writeFeatures (pw: PrintWriter, data: List[(Point, Point, List[Way])]): Unit = {

    data
      .init
      .foreach{
        case (p, p_m, lstWays) =>
          writeScenario(pw, p, p_m, lstWays)
      }

    writeScenario(pw, data.last._1, data.last._2, data.last._3)

  }

  def writeScenario (pw: PrintWriter, p: Point, p_m: Point, lstWays: List[Way]): Unit = {

    lstWays
      .map(w => wayToJSON(w))
      .foreach {
        a =>
          pw.write(a)
          pw.print(",")
      }

    pw.write(pointToJSON(p))
    pw.print(",")
    pw.write(pointToJSON(p_m))

  }

  def pointToJSON(p: Point): String = {

    write(Geometry(PointGeoJSON(p.toList)))

  }

  def wayToJSON(w: Way): String = {

    write(Geometry(WayGeoJSON(w.toListList)))

  }

}
