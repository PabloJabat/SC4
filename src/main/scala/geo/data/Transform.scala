package geo.data

import geo.elements._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

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

  def lineStringToWay(data: String): Way = {

    new Way(lineStringToPointArray(data).toList,lineStringFindWayId(data))

  }

  def lineStringToWay2 (data: String, twoWay: String): Way = {

    var twoWayBoolean = false

    if (twoWay contains "True") twoWayBoolean = true

    new Way(lineStringToPointArray(data).toList,lineStringFindWayId(data),twoWayBoolean)

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


  def filterIndexMap(lstWays: List[Way], grid: Grid): List[(String, List[Way])] = {

    lstWays
      .filter(w => grid.hasWay(w))
      .map(w => (grid.indexWay(w), w))
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}
      .groupBy{case (k,_) => k}
      .map(a => (a._1, a._2.map(b => b._2)))
      .toList

  }

  def getIndexedWaysOfIndexes(lstWays: List[(String,List[Way])], indexes: List[String]): List[(String, List[Way])] = {

    lstWays
      .filter{case (k,_) => indexes.contains(k)}

  }

  def getWaysOfIndexes(lstWays: List[(String,List[Way])], indexes: List[String]): List[Way] = {

    getIndexedWaysOfIndexes(lstWays, indexes).flatMap(a => a._2)

  }

  def filterIndexMapSpark(rddWays: RDD[Way], grid: Grid): RDD[(String, List[Way])] = {

    rddWays
      .filter(w => grid.hasWay(w))
      .map(w => (grid.indexWay(w),w))
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}
      .groupByKey
      .mapValues(_.toList)

  }

  def filterIndexGPSPointsSpark(rddGPSPoints: RDD[Point], grid: Grid): RDD[(String, Point)] = {

    rddGPSPoints
      .filter(p => grid.clearanceBoxHasPoint(p))
      .map(p => (grid.indexPoint(p),p))
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}

  }

  def joinIndexedMapPointsSpark(rddGPSPoints: RDD[(String, Point)], rddWays: RDD[(String, List[Way])]): RDD[(Point, List[Way])] = {

    rddGPSPoints.join(rddWays)
      .values
      .reduceByKey(_ ++ _)
      .mapValues(_.distinct)

  }

}
