package geo.data

import java.io._
import geo.elements._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

object Write {

  case class Empty()
  case class PolygonProperties(stroke: String,`stroke-width`: Int,`stroke-opacity`: Int,fill: String, `fill-opacity`: Int)
  case class LineProperties(stroke: String,`stroke-width`: Int,`stroke-opacity`: Int)
  case class Geometry(geometry: Any,`type`: String = "Feature", properties: Any = Empty())
  case class PointGeoJSON(coordinates: List[Double],`type`: String = "Point")
  case class WayGeoJSON(coordinates: List[List[Double]],`type`: String = "LineString")
  case class BoxGeoJSON(coordinates: List[List[List[Double]]],`type`: String = "Polygon")

  implicit val formats: DefaultFormats = DefaultFormats

  def resultsToJSON (pw: PrintWriter, data: List[(Point, Point, List[Way])], grid: Grid): Unit = {

    pw.println("{\"type\": \"FeatureCollection\" ,\"features\":[")

    writeResultsFeatures(pw, data, grid)

    pw.println("]}")

    pw.close()

  }

  def pointWaysToJSON (pw: PrintWriter, ways: List[Way], p:Point , indexes: List[String], grid: Grid): Unit = {

    pw.println("{\"type\": \"FeatureCollection\" ,\"features\":[")

    writePointWaysFeatures(pw, ways, p, indexes, grid)

    pw.println("]}")

    pw.close()

  }

  def otherWaysToJSON (pw: PrintWriter, ways: List[Way]): Unit = {

    pw.println("{\"type\": \"FeatureCollection\" ,\"features\":[")

    writeOtherWays(pw, ways)

    pw.println("]}")

    pw.close()

  }

  def cellWaysToJSON (pw: PrintWriter, lstWays: List[Way], index: String, grid: Grid): Unit = {

    pw.println("{\"type\": \"FeatureCollection\" ,\"features\":[")

    writeWaysOfCell(pw, lstWays, index, grid)

    pw.println("]}")

    pw.close()

  }

  def cellsWaysToJSON (pw: PrintWriter, lstWays: List[(String, List[Way])], grid: Grid): Unit = {

    pw.println("{\"type\": \"FeatureCollection\" ,\"features\":[")

    lstWays.init.foreach{case (index, ways) => writeWaysOfCell(pw,ways,index,grid); pw.println(",")}
    writeWaysOfCell(pw,lstWays.last._2,lstWays.last._1,grid)

    pw.println("]}")

    pw.close()

  }

  def mergedDataToJSON (pw: PrintWriter, mergedData: List[(Point,List[(String, List[Way])])], grid: Grid): Unit = {

    pw.println("{\"type\": \"FeatureCollection\" ,\"features\":[")

    mergedData.init.foreach{case (p, lstWays) => writeMergedData(pw, p, lstWays, grid); pw.println(",")}
    writeMergedData(pw, mergedData.last._1, mergedData.last._2, grid)

    pw.println("]}")

    pw.close()

  }



  def writeMergedData (pw: PrintWriter, p: Point, lstWays: List[(String, List[Way])], grid: Grid): Unit = {

    lstWays.foreach{case (index, ways) => writeWaysOfCell(pw,ways,index,grid); pw.println(",")}
    pw.println(pointToJSON(p))

  }

  def writeWaysOfCell (pw: PrintWriter, lstWays: List[Way], index: String, grid: Grid): Unit = {

    lstWays
      .map(w => wayToJSON(w) + ",")
      .foreach(w => pw.println(w))

    val cell = grid.getCellCoordinates(index)

    pw.println(boxToJSON(cell))

  }

  def writeOtherWays (pw: PrintWriter, ways: List[Way]): Unit = {

    val myProperties = LineProperties("#ec5757",2,1)

    ways
      .init
      .map(w => wayToJSON(w, myProperties))
      .foreach {
        a =>
          pw.println(a + ",")
      }

    pw.println(wayToJSON(ways.last, myProperties))

  }

  def writePointWaysFeatures(pw: PrintWriter, ways: List[Way], p: Point, indexes: List[String], grid: Grid): Unit = {

    ways.foreach(w => pw.println(wayToJSON(w)+","))
    val boxes = indexes.map(i => grid.getCellCoordinates(i))

    boxes
      .map(box => boxToJSON(box))
      .foreach {
        a =>
          pw.println(a + ",")
      }

    pw.println(pointToJSON(p))

  }

  def writeResultsFeatures (pw: PrintWriter, data: List[(Point, Point, List[Way])], grid: Grid): Unit = {

    data
      .init
      .foreach{
        case (p, p_m, lstWays) =>
          writeScenario(pw, p, p_m, lstWays, grid)
          pw.println(",")
      }

    writeScenario(pw, data.last._1, data.last._2, data.last._3, grid)

  }

  def writeScenario (pw: PrintWriter, p: Point, p_m: Point, lstWays: List[Way], grid: Grid): Unit = {

    lstWays
      .map(w => wayToJSON(w))
      .foreach {
        a =>
          pw.println(a + ",")
      }

    val boxes = grid.indexPoint(p)
      .map(index => grid.getCellCoordinates(index))

    boxes
      .map(box => boxToJSON(box))
      .foreach {
        a =>
          pw.println(a + ",")
      }

    pw.println(pointToJSON(p) + ",")
    pw.println(pointToJSON(p_m))

  }



  def pointToJSON(p: Point): String = {

    val vector = List(p.toList,p.computePointDistanceBearing(15).toList)

    write(Geometry(PointGeoJSON(p.toList))) + "," + write(Geometry(WayGeoJSON(vector)))

  }

  def wayToJSON(w: Way, properties: Any = Empty()): String = {

    write(Geometry(WayGeoJSON(w.toListList),properties = properties))

  }

  def boxToJSON(points: List[Point]): String = {

    val myProperties = PolygonProperties("#555555",2,1,"#555555",0)
    val listPoints = List(points.map(_.toList))
    write(Geometry(BoxGeoJSON(listPoints),properties = myProperties))

  }

}
