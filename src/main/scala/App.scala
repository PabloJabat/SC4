import java.net.{URI => JavaURI}

import functions._
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}
import org.apache.spark.sql.SparkSession

//import scala.collection.mutable
import scala.math.{Pi, abs, acos, min, pow, sqrt}

class Point (val x: Double, val y: Double) extends Serializable{

  //The x is the Latitude and y is the Longitude
  override def toString: String = "Point(" + x + "," + y + ")"
}
class Segment (val a: Point, val b: Point) extends Serializable{
}
class Way (val points: List[Point]) extends Serializable{

}

object functions {

  def dataTransform(list: Array[String]): Point = {
    //We first put the 4th entry as it is the latitude and we want the LatLon array
    new Point(list(4).toDouble, list(3).toDouble)
  }

  def area(a: Point, b: Point, c: Point): Double = {
    abs(a.x * (b.y - c.y) + b.x * (c.y - a.y) + c.x * (a.y - b.y)) / 2
  }

  def norm(a: Point, b: Point): Double = {
    sqrt(pow(a.x - b.x, 2) + pow(a.y - b.y, 2))
  }

  def height(p: Point, segment: (Point, Point)): Double = {
    area(segment._1, segment._2, p) * 2 / norm(segment._1, segment._2)
  }

  def distanceToSegment(p: Point, segment: (Point, Point)): Double = {
    val h = height(p, segment)
    val dist = min(norm(segment._1, p),norm(segment._2, p))
    if (outOfSegment(p, segment)) min(h,dist)
    else h
  }

  def outOfSegment(p: Point, segment: (Point, Point)): Boolean = {
    val p_a = p
    val p_b = segment._1
    val p_c = segment._2

    val a = norm(p_b, p_c)
    val b = norm(p_c, p_a)
    val c = norm(p_a, p_b)

    val beta = acos((pow(a,2)+pow(c,2)-pow(b,2))/(2*a*c))
    val gamma = acos((pow(a,2)+pow(b,2)-pow(c,2))/(2*a*b))

    if (beta > Pi/2 || gamma > Pi/2) true else false
  }

  def stringToPoint(data: String): Point = {

    //the x is the latitude and y is the longitude that is why we switch the order
    val pattern = "([0-9]+.[0-9]+) ([0-9]+.[0-9]+)".r
    data match {
      case pattern(y, x) => new Point(x.toDouble, y.toDouble)
    }
  }

  def PointArrayToSegmentsArray(data: Array[Point]): Array[(Point, Point)] = {
    val dataZipped = data.zipWithIndex
    val dataResult = for ((e, i) <- dataZipped if i != 0)
      yield {
        (dataZipped(i - 1)._1, e)
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

  //This function below allows us to obtain an array of all the paired points that we can find in the map (we have paired points within a segment)
  def lineStringToSegmentArray(data: String): Array[(Point, Point)] = {
    val pattern = "[0-9]+.[0-9]+ [0-9]+.[0-9]+".r
    val segArray = pattern.findAllIn(data).toArray
    PointArrayToSegmentsArray(segArray.map(stringToPoint))
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
    val minLatWay = way.map(p => p.x).min
    val maxLatWay = way.map(p => p.x).max
    val minLonWay = way.map(p => p.y).min
    val maxLonWay = way.map(p => p.y).max

    //We now create a box similar to the Grid Box so that me can use it to compare it with each way's Box
    val wayBox = Array(new Point(minLatWay, minLonWay),
      new Point(minLatWay, maxLonWay),
      new Point(maxLatWay, minLonWay),
      new Point(maxLatWay, maxLonWay))

    //Here we compare the way's Grid Box and with the one passed to the function
    //We return a true if they overlap, otherwise, we return a false
    for {point <- wayBox
         testLat = (minLat <= point.x) && (maxLat >= point.x)
         testLon = (minLon <= point.y) && (maxLon >= point.y)} {

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
    val latTest = (i: Int) => (point.x >= ((i - 1) * latDiv + minOmsLat)) && (point.x <= (i * latDiv + minOmsLat))
    val lonTest = (j: Int) => (point.y >= ((j - 1) * lonDiv + minOmsLon)) && (point.y <= (j * lonDiv + minOmsLon))

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

  def gridBoxOfSegment (osmBox: (Double, Double, Double, Double), n: Int, m: Int, segment: (Point, Point)): Array[String] = {

    val minLatSegment = List(segment._1.x,segment._2.x).min
    val maxLatSegment = List(segment._1.x,segment._2.x).max
    val minLonSegment = List(segment._1.y,segment._2.y).min
    val maxLonSegment = List(segment._1.y,segment._2.y).max

    //We now create a box consisting of 4 points similar to the Grid Box so that me can use it to compare it with each segments's Box
    val segmentBox = Array(new Point(minLatSegment, minLonSegment),
      new Point(minLatSegment, maxLonSegment),
      new Point(maxLatSegment, minLonSegment),
      new Point(maxLatSegment, maxLonSegment))

    segmentBox.map(gridBoxOfPoint(osmBox,n,m,_)).distinct
  }

  def gridBoxOfWay (osmBox: (Double, Double, Double, Double), n: Int, m: Int, way: Array[Point]): Array[String] = {

    //Remember that the Point.x is the Latitude and Point.y is the Longitude
    val minLatWay = way.map(p => p.x).min
    val maxLatWay = way.map(p => p.x).max
    val minLonWay = way.map(p => p.y).min
    val maxLonWay = way.map(p => p.y).max

    //We now create a box consisting of 4 points similar to the Grid Box so that me can use it to compare it with each way's Box
    val wayBox = Array(new Point(minLatWay, minLonWay),
      new Point(minLatWay, maxLonWay),
      new Point(maxLatWay, minLonWay),
      new Point(maxLatWay, maxLonWay))

    wayBox.map(gridBoxOfPoint(osmBox,n,m,_)).distinct
  }

  def coordinatesDifference (segment: (Point, Point)): (Double, Double) = {
    val latDifference = abs(segment._1.x - segment._2.x)
    val lonDifference = abs(segment._1.y - segment._2.y)
    (latDifference, lonDifference)
  }

  def isPointInRegion (gpsPoint: Point, osmBox: (Double, Double, Double, Double)): Boolean = {

    //Remember that the box tuple contains coordinates information in the following order:
    //(minLat, maxLat, minLon, maxLon)
    val minOmsLat = osmBox._1
    val maxOmsLat = osmBox._2
    val minOmsLon = osmBox._3
    val maxOmsLon = osmBox._4

    val test = (gpsPoint.x > minOmsLat && gpsPoint.x < maxOmsLat) && (gpsPoint.y > minOmsLon && gpsPoint.y < maxOmsLon)
    test
  }

  def isSegmentInRegion (segment: (Point, Point), omsBox: (Double, Double, Double, Double)): Boolean = {

    //Remember that the box tuple contains coordinates information in the following order:
    //(minLat, maxLat, minLon, maxLon)

    val test = isPointInRegion(segment._1, omsBox) || isPointInRegion(segment._2, omsBox)
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

  def projectToSegment (p: Point, segment: (Point, Point)): Point = {
    if (outOfSegment(p, segment)) {
      if (norm(p, segment._1) > norm(p, segment._2)) segment._2 else segment._1
    } else {
      val bigHyp = norm(segment._1, segment._2)
      val smallHyp = sqrt(pow(norm(p,segment._1),2)-pow(height(p, segment),2))
      val (diffLat, diffLon) = coordinatesDifference(segment._1, segment._2)
      val incrLat = smallHyp * diffLat / bigHyp
      val incrLon = smallHyp * diffLon / bigHyp
      new Point(p.x + incrLat, p.y + incrLon)
    }
  }

  def mapMatching (p: Point, segments: List[(Point,Point)]): Point = {
    val bestCandidate = segments.map(s => (s, distanceToSegment(p,s))).reduce((a,b) => if (a._2 < b._2) a else b)
    projectToSegment(p, bestCandidate._1)
  }
}

object MapMatching {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in1, config.in2)
      case None =>
        println(parser.usage)
    }
  }

  def run (osm_data: String, gps_data: String) = {

//    val osm_data = args(0)
//    val gps_data = args(1)

    val spark = SparkSession.builder
      .appName(s"Triple reader example")
      .master("local[*]") // spark url
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc = spark.sparkContext
    val ops = JenaSparkRDDOps(sc)
    import ops._

    println("======================================")
    println("|         Simple Map Matching        |")
    println("======================================")

    val osmBox = (40.6280, 40.6589, 22.9182, 22.9589)
    val gpsData = sc.textFile(gps_data)
      .map(_.split("\\s+"))
      .map(dataTransform)
      .filter(isPointInRegion(_,osmBox))

    val mapData: TripleRDD = NTripleReader.load(spark, JavaURI.create(osm_data))

    val waysTriples = mapData
      .find(ANY,URI("http://www.opengis.net/ont/geosparql#asWKT"),ANY)

    val waysData = waysTriples
      .map(a => a.getObject.toString())

    val segmentsData = waysData
      .flatMap(lineStringToSegmentArray)
      .filter(isSegmentInRegion(_,osmBox))

    val minResolution = segmentsData
      .map(coordinatesDifference)
      .reduce((a,b) => (List(a._1,b._1).max, List(a._2,b._2).max))

    val resolution = (minResolution._1,minResolution._2)
    val divisions = computeDivisions(osmBox, resolution)

    val gpsDataIndexed = gpsData
      .map(p => (gridBoxOfPoint(osmBox,divisions._1,divisions._2,p),p))

    val segmentsDataIndexed = segmentsData
      .map(p => (gridBoxOfSegment(osmBox,divisions._1, divisions._2, p),p))
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}
      .groupByKey
      .mapValues(_.toList)

    val mergedData = gpsDataIndexed
      .join(segmentsDataIndexed)
      .map(a => a._2)

    val matchedData = mergedData
      .map{case (p: Point, segments: List[(Point,Point)]) => (mapMatching(p,segments),p)}

    matchedData.take(5).foreach{
      case (mp: Point, op: Point) => println("Map Matched Point: " + mp.toString);
        println("Original Point: " + op.toString)}

    spark.stop
  }

  case class Config(in1: String = "", in2: String = "")

  val parser = new scopt.OptionParser[Config]("Simple Map Matching") {

    head("Simple Map Matching")

    opt[String]('m', "map").required().valueName("<path>").
      action((x, c) => c.copy(in1 = x)).
      text("path to file that contains the map data (in N-Triples format)")

    opt[String]('i', "gps").required().valueName("<path>").
      action((x, c) => c.copy(in2 = x)).
      text("path to file that contains the gps data (in .csv format)")

    help("help").text("prints this usage text")
  }
}
