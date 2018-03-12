package geo.data

import geo.elements.{Point, Segment, Way}
import geo.math.algebra._

import scala.collection.mutable.ListBuffer
import scala.math.abs

object Transform {

  def lineHasWay (line: String): Boolean = {

    val pattern = "<http://www.opengis.net/ont/geosparql#asWKT>".r

    pattern.findFirstIn(line).nonEmpty

  }

  def toWay (lstSegments: List[Segment]): Way = {

    val pointsList = ListBuffer(lstSegments.head.a)

    for (i <- lstSegments.indices) {

      pointsList += lstSegments(i).b

    }

    new Way(pointsList.toList, "")

  }

  def stringToPoint(data: String): Point = {

    //the x is the latitude and y is the longitude that is why we switch the order
    val pattern = "([0-9]+.[0-9]+) ([0-9]+.[0-9]+)".r
    data match {
      case pattern(y, x) => new Point(x.toDouble, y.toDouble)
    }
  }

  def pointArrayToSegmentsArray(data: Array[Point]): Array[Segment] = {

    val dataZipped = data.zipWithIndex
    val dataResult = for ((e, i) <- dataZipped if i != 0)
      yield {
        new Segment(dataZipped(i - 1)._1, e)
      }
    dataResult
  }

  def lineStringToPointStringArray(data: String): Array[String] = {

    val pattern = "[0-9]+.[0-9]+ [0-9]+.[0-9]+".r
    pattern.findAllIn(data).toArray

  }

  def lineStringToPointArray(data: String): Array[Point] = {

    val pattern = "[0-9]+.[0-9]+ [0-9]+.[0-9]+".r
    val segArray = pattern.findAllIn(data).toArray
    segArray.map(stringToPoint)

  }

  def lineStringToSegmentArray(data: String): Array[Segment] = {

    val pattern = "[0-9]+.[0-9]+ [0-9]+.[0-9]+".r
    val segArray = pattern.findAllIn(data).toArray
    pointArrayToSegmentsArray(segArray.map(stringToPoint))

  }

  def lineStringtoWay(data: String): Way = {

    new Way(lineStringToPointArray(data).toList,lineStringFindWayId(data))

  }

  def lineStringFindWayId(line: String): String = {

    val pattern = "<urn:osm:way:geometry:uuid:([0-9]+)> (.*)".r
    val pattern(id,_) = line
    id

  }

  def findWayID(way: String): String = {

    val pattern = "urn:osm:way:geometry:uuid:([0-9]+)".r
    val pattern(id) = way
    id

  }

}
