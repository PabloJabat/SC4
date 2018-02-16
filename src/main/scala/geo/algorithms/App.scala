package geo.algorithms

import java.net.{URI => JavaURI}

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}
import org.apache.spark.sql._

import geo.elements._
import geo.data.Transform._
import geo.data.Read._
import geo.algorithms.MapMatching._

object App {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in1, config.in2)
      case None =>
        println(parser.usage)
    }

  }

  def run (osm_data: String, gps_data: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"Simple Map Matching")
      .master("local[*]") // spark url
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc = spark.sparkContext
    val ops = JenaSparkRDDOps(sc)
    import ops._

    //We need it if we want to transform tuple RDD to DF
    import spark.implicits._

    println("======================================")
    println("|         Simple Map Matching        |")
    println("======================================")

    val pattern = """([^";]+)""".r

    val osmBox = (40.6280, 40.6589, 22.9182, 22.9589)
    val gpsData = sc.textFile(gps_data)
      .map(a => pattern.findAllIn(a).toList)
      .map(dataExtraction)
      .filter(isPointInRegion(_,osmBox))

    val mapData: TripleRDD = NTripleReader.load(spark, JavaURI.create(osm_data))

    val waysTriples = mapData
      .find(ANY,URI("http://www.opengis.net/ont/geosparql#asWKT"),ANY)

    val waysData = waysTriples
      .map(a => (findWayID(a.getSubject.toString),a.getObject.toString))

    val segmentsData = waysData
      .flatMapValues(lineStringToSegmentArray)
      .filter(a => isSegmentInRegion(a._2,osmBox))

    val minResolution = segmentsData
      .map(a => coordinatesDifference(a._2))
      .reduce((a,b) => (List(a._1,b._1).max, List(a._2,b._2).max))

    val resolution = (minResolution._1,minResolution._2)
    val divisions = computeDivisions(osmBox, resolution)

    val gpsDataIndexed = gpsData
      .map(p => (gridBoxOfPoint(osmBox,divisions._1,divisions._2,p),p))

    val segmentsDataIndexed = segmentsData
      .map(s => (gridBoxOfSegment(osmBox,divisions._1, divisions._2, s._2),s))
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}
      .groupByKey
      .mapValues(_.toList)

    val mergedData = gpsDataIndexed
      .join(segmentsDataIndexed)
      .map(a => a._2)

    val matchedData = mergedData
      .map{case (p: Point, s: List[(String, Segment)]) => (naiveBayesClassifierMM(p,s),p)}
      .map{case ((wayId, new_p), p) => (wayId, p, new_p)}


    matchedData.map{case (wayId, p, new_p) => (wayId, p.id, p.lat, p.lon, new_p.lat, new_p.lon)}
      .toDF("waysID","pointID","latitude","longitude","matched latitude","matched longitude")
      .coalesce(1).write.csv("/home/pablo/results")

//    matchedData.take(5).foreach{
//      case (id, p, new_p) =>
//        println("Id of way: " + id.toString)
//        println("Map Matched Point: " + new_p.toString)
//        println("Original Point: " + p.toString)}

    spark.stop

  }

  case class Config(in1: String = "", in2: String = "")

  val parser: scopt.OptionParser[Config] = new scopt.OptionParser[Config]("Simple Map Matching") {

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
