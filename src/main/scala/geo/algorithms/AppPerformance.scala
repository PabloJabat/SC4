package geo.algorithms

import org.apache.spark.sql.SparkSession
import geo.data.Read._
import java.io._

object AppPerformance {

  def main(args: Array[String]) {

    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in1, config.in2)
      case None =>
        println(parser.usage)

    }

    def run (gps_matched_data: String, gps_data: String): Unit = {

      val spark = SparkSession
        .builder
        .appName(s"Simple Map Matching")
        .master("local[*]") // spark url
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()

      val sc = spark.sparkContext

      println("======================================")
      println("|   Simple Map Matching Performance  |")
      println("======================================")

      val matchedData = sc.textFile(gps_matched_data)
        .map(_.split(",").toList)
        .map(matchedGPSDataExtraction)

      val refData = sc.textFile(gps_data)
        .map(_.split(",").toList)
        .map(referenceGPSDataExtraction)

      val joinedData = refData.join(matchedData)

      val joinedDataCount = joinedData.count()

      joinedData.persist()

      val correctMatches = joinedData
        .filter{case (_, waysID) => waysID._2 == waysID._1}
        .count()

      val performance = (correctMatches*10000/joinedDataCount).toDouble/100

      println("% Performance: " + performance)

      val incorrectMatches = joinedData
        .filter{case (_, waysID) => !(waysID._2 == waysID._1)}

      val pw = new PrintWriter("/home/pablo/DE/DataSets/incorrectMatches.txt")

      incorrectMatches
        .take(10)
        .map(a => a._1)
        .foreach(a => pw.println(a))

      pw.close()

    }

  }

  case class Config(in1: String = "", in2: String = "")

  val parser: scopt.OptionParser[Config] = new scopt.OptionParser[Config]("Simple Map Matching Performance") {

    head("Simple Map Matching Performance")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in1 = x)).
      text("path to file that contains the gps data (in .csv format)")

    opt[String]('o', "output").required().valueName("<path>").
      action((x, c) => c.copy(in2 = x)).
      text("path to file that contains the gps matched data (in .csv format)")

    help("help").text("prints this usage text")

  }

}
