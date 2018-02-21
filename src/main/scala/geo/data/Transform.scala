package geo.data

import geo.elements.{Point, Segment, Way}

import scala.collection.mutable.ListBuffer
import scala.math.abs

object Transform {

  def toWay (lstSegments: List[Segment]): Way = {

    val pointsList = ListBuffer(lstSegments.head.a)

    for (i <- lstSegments.indices) {

      pointsList += lstSegments(i).b

    }

    new Way(pointsList.toList)

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

  def findWayID(way: String): String = {
    val pattern = "urn:osm:way:geometry:uuid:([0-9]+)".r
    val pattern(id) = way
    id
  }

  def getFirstNode(way: Array[Point]): Point = {
    way(0)
  }

  def getLastNode(way: Array[Point]): Point = {
    way(way.length - 1)
  }

  def getFirsLastNode(way: String): String = {

    //This function appends firstNode and lastNode to the segment string

    val point = "[0-9]+.[0-9]+ [0-9]+.[0-9]+"
    val patternFirst = point.r
    val patternLast = (point + "(?!.*" + point + ")").r
    val firstNode = patternFirst.findFirstIn(way).getOrElse()
    val lastNode = patternLast.findFirstIn(way).getOrElse()
    way + "firstNode((" + firstNode + "))" + "lastNode((" + lastNode + "))"

  }

  def isInGridBox(box: (Double, Double, Double, Double), way: Array[Point]): Boolean = {

    //The box parameters must be passed to the function
    //in such a way that it matches the variables name:
    //(minLat, maxLat, minLon, maxLon)
    val minLat = box._1
    val maxLat = box._2
    val minLon = box._3
    val maxLon = box._4

    //Remember that the Point.x is the Latitude and Point.y is the Longitude
    val minLatWay = way.map(p => p.lat).min
    val maxLatWay = way.map(p => p.lat).max
    val minLonWay = way.map(p => p.lon).min
    val maxLonWay = way.map(p => p.lon).max

    //We now create a box similar to the Grid Box so that me can use it to compare it with each way's Box
    val wayBox = Array(new Point(minLatWay, minLonWay),
      new Point(minLatWay, maxLonWay),
      new Point(maxLatWay, minLonWay),
      new Point(maxLatWay, maxLonWay))

    //Here we compare the way's Grid Box and with the one passed to the function
    //We return a true if they overlap, otherwise, we return a false
    for {point <- wayBox
         testLat = (minLat <= point.lat) && (maxLat >= point.lat)
         testLon = (minLon <= point.lon) && (maxLon >= point.lon)} {

      if (testLat && testLon) {

        return true

      }
    }

    false

  }

  def waysInGridBox(box: (Double, Double, Double, Double), ways: Array[(Int, Array[Point])]): Array[Int] = {

    //Remember that the box tuple contains coordinates information in the following order:
    //(minLat, maxLat, minLon, maxLon)
    for ((id, way) <- ways if isInGridBox(box, way)) yield id

  }

  def gridBoxOfPoint(osmBox: (Double, Double, Double, Double), n: Int, m: Int, point: Point): String = {

    //Remember that the box tuple contains coordinates information in the following order:
    //(minLat, maxLat, minLon, maxLon)
    val minOmsLat = osmBox._1
    val maxOmsLat = osmBox._2
    val minOmsLon = osmBox._3
    val maxOmsLon = osmBox._4

    //Variable n refers to the number of latitude divisions
    //Variable m refers to the number of longitude divisions
    val latDiv = abs(maxOmsLat - minOmsLat) / n
    val lonDiv = abs(maxOmsLon - minOmsLon) / m

    //We initialize the 2 variables which will be returned (a for the latitude division and b for the longitude division)
    var a = ""
    var b = ""

    //Tests
    val latTest = (i: Int) => (point.lat >= ((i - 1) * latDiv + minOmsLat)) && (point.lat <= (i * latDiv + minOmsLat))
    val lonTest = (j: Int) => (point.lon >= ((j - 1) * lonDiv + minOmsLon)) && (point.lon <= (j * lonDiv + minOmsLon))

    for (i <- 1 to n if latTest(i)) a = i.toString
    for (j <- 1 to n if lonTest(j)) b = j.toString

    a+b

    //    val minLat = (a - 1) * latDiv + minOmsLat
    //    val maxLat = a * latDiv + minOmsLat
    //    val minLon = (b - 1) * lonDiv + minOmsLat
    //    val maxLon = b * lonDiv + minOmsLat
    //
    //    (minLat, maxLat, minLon, maxLon)
  }

  def gridBoxOfSegment (osmBox: (Double, Double, Double, Double), n: Int, m: Int, s: Segment): Array[String] = {

    val minLatSegment = List(s.a.lat,s.b.lat).min
    val maxLatSegment = List(s.a.lat,s.b.lat).max
    val minLonSegment = List(s.a.lon,s.b.lon).min
    val maxLonSegment = List(s.a.lon,s.b.lon).max

    //We now create a box consisting of 4 points similar to the Grid Box so that me can use it to compare it with each segments's Box
    val segmentBox = Array(new Point(minLatSegment, minLonSegment),
      new Point(minLatSegment, maxLonSegment),
      new Point(maxLatSegment, minLonSegment),
      new Point(maxLatSegment, maxLonSegment))

    segmentBox.map(gridBoxOfPoint(osmBox,n,m,_)).distinct
  }

  def gridBoxOfWay (osmBox: (Double, Double, Double, Double), n: Int, m: Int, way: Array[Point]): Array[String] = {

    //Remember that the Point.x is the Latitude and Point.y is the Longitude
    val minLatWay = way.map(p => p.lat).min
    val maxLatWay = way.map(p => p.lat).max
    val minLonWay = way.map(p => p.lon).min
    val maxLonWay = way.map(p => p.lon).max

    //We now create a box consisting of 4 points similar to the Grid Box so that me can use it to compare it with each way's Box
    val wayBox = Array(new Point(minLatWay, minLonWay),
      new Point(minLatWay, maxLonWay),
      new Point(maxLatWay, minLonWay),
      new Point(maxLatWay, maxLonWay))

    wayBox.map(gridBoxOfPoint(osmBox,n,m,_)).distinct
  }

  def coordinatesDifference (s: Segment): (Double, Double) = {

    val latDifference = abs(s.a.lat - s.b.lat)
    val lonDifference = abs(s.a.lon - s.b.lon)
    (latDifference, lonDifference)

  }

  def isPointInRegion (gpsPoint: Point, osmBox: (Double, Double, Double, Double)): Boolean = {

    //Remember that the box tuple contains coordinates information in the following order:
    //(minLat, maxLat, minLon, maxLon)
    val minOmsLat = osmBox._1
    val maxOmsLat = osmBox._2
    val minOmsLon = osmBox._3
    val maxOmsLon = osmBox._4

    val test = (gpsPoint.lat > minOmsLat && gpsPoint.lat < maxOmsLat) && (gpsPoint.lon > minOmsLon && gpsPoint.lon < maxOmsLon)
    test
  }

  def isSegmentInRegion (s: Segment, omsBox: (Double, Double, Double, Double)): Boolean = {

    //Remember that the box tuple contains coordinates information in the following order:
    //(minLat, maxLat, minLon, maxLon)

    val test = isPointInRegion(s.a, omsBox) || isPointInRegion(s.b, omsBox)
    test

  }

  def computeDivisions (osmBox: (Double, Double, Double, Double), resolution: (Double, Double)): (Int, Int) = {

    //Remember that the box tuple contains coordinates information in the following order:
    //(minLat, maxLat, minLon, maxLon)

    val latLength = osmBox._2 - osmBox._1
    val lonLength = osmBox._4 - osmBox._3

    val n = (latLength/resolution._1).ceil.toInt
    val m = (lonLength/resolution._2).ceil.toInt
    (n, m)
  }

}
