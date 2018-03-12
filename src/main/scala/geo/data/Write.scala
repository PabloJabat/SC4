package geo.data

import java.io._
import geo.elements._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

object Write {

  case class Empty()
  case class Geometry(geometry: Any,`type`: String = "Feature", properties: Any = Empty())

  case class PolygonProperties(stroke: String,`stroke-width`: Int,`stroke-opacity`: Int,fill: String, `fill-opacity`: Int)
  case class LineStringProperties(stroke: String,`stroke-width`: Int,`stroke-opacity`: Int)

  case class PointGeoJSON(coordinates: List[Double],`type`: String = "Point")
  case class WayGeoJSON(coordinates: List[List[Double]],`type`: String = "LineString")
  case class BoxGeoJSON(coordinates: List[List[List[Double]]],`type`: String = "Polygon")

  implicit val formats: DefaultFormats = DefaultFormats

  private def pointToJSON (p: Point): String = {

    val vector = List(p.toList,p.computePointDistanceBearing(15).toList)

    write(Geometry(PointGeoJSON(p.toList))) + "," + write(Geometry(WayGeoJSON(vector)))

  }

  private def wayToJSON (w: Way, properties: Any = Empty()): String = {

    write(Geometry(WayGeoJSON(w.toListList),properties = properties))

  }

  private def boxToJSON (points: List[Point]): String = {

    val myProperties = PolygonProperties("#555555",2,1,"#555555",0)
    val listPoints = List(points.map(_.toList))
    write(Geometry(BoxGeoJSON(listPoints),properties = myProperties))

  }

  private def insertComma (lines: List[String]): List[String] = {

    lines.init.map(_ + ",") ++ List(lines.last)

  }

  private def writeToFile (pw: PrintWriter, lines: List[String]): Unit = {

    pw.println("{\"type\": \"FeatureCollection\" ,\"features\":[")

    insertComma(lines).foreach(line => pw.println(line))

    pw.println("]}")

    pw.close()

  }

  def indexedDataToJSON (pw: PrintWriter, indexedData: List[(Point,List[Way])], grid: Grid): Unit = {

    val points = indexedData
      .map(r => pointToJSON(r._1))

    val cells = indexedData
      .map(r => r._1)
      .flatMap(p => grid.indexPoint(p))
      .map(index => grid.getCellCoordinates(index))
      .map(cell => boxToJSON(cell))

    val ways = indexedData
      .flatMap(r => r._2)
      .map(w => wayToJSON(w))

    writeToFile(pw, points ++ cells ++ ways)

  }

  def resultsToJSON (pw: PrintWriter, results: List[(Point, Point, List[Way])], grid: Grid): Unit = {

    val points = results
      .map(r => pointToJSON(r._1))

    val matchedPoints = results
      .map(r => pointToJSON(r._2))

    val cells = results
      .map(r => r._1)
      .flatMap(p => grid.indexPoint(p))
      .map(index => grid.getCellCoordinates(index))
      .map(cell => boxToJSON(cell))

    val ways = results
      .flatMap(r => r._3)
      .map(w => wayToJSON(w))

    writeToFile(pw, points ++ cells ++ ways ++ matchedPoints)

  }

  def cellsToJSON (pw: PrintWriter, waysIndexed: List[(String, List[Way])], grid: Grid): Unit = {

    val cells = waysIndexed
      .map(w => w._1)
      .map(index => grid.getCellCoordinates(index))
      .map(cell => boxToJSON(cell))

    val ways = waysIndexed
      .flatMap(w => w._2)
      .map(w => wayToJSON(w))

    writeToFile(pw, cells ++ ways)

  }

}
