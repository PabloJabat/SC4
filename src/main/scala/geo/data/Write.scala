package geo.data

import java.io._
import geo.elements._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

object Write {

  def writeToJSON (data: ((String, Segment),Point, Point, List[(String,Segment)]), pw: PrintWriter): Unit = {

    implicit val formats = DefaultFormats

    case class Empty()
    //case class Features (`type`:String = "FeatureCollection", features: List[Geometry])
    case class Geometry(geometry: Any,`type`: String = "Feature", properties: Any = Empty())
    case class PointGeoJSON(coordinates: List[Double],`type`: String = "Point")
    case class WayGeoJSON(coordinates: List[List[Double]],`type`: String = "LineString")

    val seg = Geometry(
      WayGeoJSON(
        List(data._1._2.a.toList, data._1._2.b.toList)
      )
    )

    val originalPoint = Geometry(
      PointGeoJSON(
        data._2.toList
      )
    )

    val matchedPoint = Geometry(
      PointGeoJSON(
        data._3.toList
      )
    )

    val lstGeometry = List(seg, originalPoint, matchedPoint)

    lstGeometry
      .zipWithIndex
      .foreach {
        case (a, i) =>
          pw.println(write(a))
          if (i != 2) pw.println(",")
      }

    //val jsonString = write(lstGeometry)
    //val jsonString = write(Features(features = lstGeometry))

    //pw.println(jsonString)

  }

}
