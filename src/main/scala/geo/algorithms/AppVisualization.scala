package geo.algorithms

import java.io.PrintWriter
import java.net.{URI => JavaURI}

import geo.algorithms.MapMatching._
import geo.data.Read.dataExtraction
import geo.data.Transform._
import geo.data.Write.resultsToJSON
import geo.elements.{BoxLimits, Grid, Way}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object AppVisualization {

  def main(args: Array[String]) {

    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in1, config.in2, config.in3)
      case None =>
        println(parser.usage)
    }

    def run(gps_data: String, osm_data: String, mismatchedPoints: String): Unit = {

      val spark = SparkSession
        .builder
        .appName(s"Simple Map Matching")
        .master("local[*]") // spark url
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()

      val sc = spark.sparkContext
      val ops = JenaSparkRDDOps(sc)
      import ops._

      println("========================================")
      println("|   Simple Map Matching Visualization  |")
      println("========================================")

      val pattern = """([^";]+)""".r

      println(gps_data+": -> gpsData")
      println(osm_data+": -> osmData")
      println(mismatchedPoints + ": -> mismatched")

      val osmBox = BoxLimits(40.6486, 40.6374, 22.9478, 22.9298)
      val myGrid = new Grid(osmBox,20)

      val gpsData = sc.textFile(gps_data)
        .map(line => pattern.findAllIn(line).toList)
        .map(dataExtraction)
        .filter(p => myGrid.clearanceBoxHasPoint(p))

      val mapData: TripleRDD = NTripleReader.load(spark, JavaURI.create(osm_data))

      val waysTriples = mapData
        .find(ANY,URI("http://www.opengis.net/ont/geosparql#asWKT"),ANY)

      val waysData = waysTriples
        .map(a => (findWayID(a.getSubject.toString),a.getObject.toString))
        .map{case (id, way) => new Way(lineStringToPointArray(way).toList,id)}

      val incorrectMatchesData = Source.fromFile(mismatchedPoints).getLines().toList

      val gpsDataIndexed = gpsData
        .map(p => (myGrid.indexPoint(p),p))
        .filter(a => incorrectMatchesData.contains(a._2.id))
        .flatMap{case (k,v) => for (i <- k) yield (i, v)}

      val waysDataIndexed = waysData
        .map(w => (myGrid.indexWay(w),w))
        .flatMap{case (k,v) => for (i <- k) yield (i, v)}
        .groupByKey
        .mapValues(_.toList)

      val mergedData = gpsDataIndexed
        .join(waysDataIndexed)
        .values
        .groupByKey
        .mapValues(_.toList)
        .mapValues(_.flatten)

      val matchedData = mergedData.map{case (p, lstWays) => (geometricMM2(p,lstWays),p,lstWays)}

      val resultsToVisualize = matchedData
        .take(1)
        .map{case ((w, m_p),p,lstW) => (w,p,m_p,lstW,myGrid.indexPoint(p))}


      val pw = new PrintWriter("/home/pablo/mismatchedData.json")
      val jsonResults = resultsToVisualize
        .map(a => (a._2,a._3,a._4)).toList

      resultsToJSON(pw, jsonResults, myGrid)

    }

  }

    case class Config(in1: String = "", in2: String = "", in3: String = "")

    val parser: scopt.OptionParser[Config] = new scopt.OptionParser[Config]("Simple Map Matching Performance") {

      head("Visualization of the GPS data")

      opt[String]('i', "input").required().valueName("<path>").
        action((x, c) => c.copy(in1 = x)).
        text("path to file that contains the gps data (in .csv format)")

      opt[String]('m', "map").required().valueName("<path>").
        action((x, c) => c.copy(in2 = x)).
        text("path to file that contains the osm data (in .nt format)")

      opt[String]('n', "incorrect").required().valueName("<path>").
        action((x, c) => c.copy(in3 = x)).
        text("path to file that contains the mismatched data timestamp + id")

      help("help").text("prints this usage text")

    }

}
