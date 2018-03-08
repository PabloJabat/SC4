package geo.algorithms

import geo.data.Transform.findWayID
import geo.data.Read.matchedGPSDataExtractionStats
import org.apache.spark.sql.SparkSession
import java.net.{URI => JavaURI}

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}

object AppStatistics {

  def main(args: Array[String]) {

    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in1, config.in2)
      case None =>
        println(parser.usage)

    }

    def run(gps_matched_data: String, osm_data: String): Unit = {

      val spark = SparkSession
        .builder
        .appName(s"Simple Map Matching")
        .master("local[*]") // spark url
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()

      val sc = spark.sparkContext
      val ops = JenaSparkRDDOps(sc)
      import ops._

      println("=======================================")
      println("|      Statistics of the GPS data     |")
      println("=======================================")

      val osmBox = (40.6280, 40.6589, 22.9182, 22.9589)

      val matchedData = sc.textFile(gps_matched_data)
        .map(_.split(",").toList)
        .map(matchedGPSDataExtractionStats)

      val mapData: TripleRDD = NTripleReader.load(spark, JavaURI.create(osm_data))

      val waysTriples = mapData
        .find(ANY,URI("http://www.opengis.net/ont/geosparql#asWKT"),ANY)

      val waysData = waysTriples
        .map(a => (findWayID(a.getSubject.toString),a.getObject.toString))

    }

  }

    case class Config(in1: String = "", in2: String = "")

    val parser: scopt.OptionParser[Config] = new scopt.OptionParser[Config]("Simple Map Matching Performance") {

      head("Statistics of the GPS data")

      opt[String]('r', "reference").required().valueName("<path>").
        action((x, c) => c.copy(in1 = x)).
        text("path to file that contains the reference matched gps data (in .csv format)")

      opt[String]('m', "map").required().valueName("<path>").
        action((x, c) => c.copy(in2 = x)).
        text("path to file that contains the osm data (in .nt format)")

      help("help").text("prints this usage text")

    }

}
