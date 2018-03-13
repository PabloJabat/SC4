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

      println("======================================")
      println("|   Simple Map Matching Performance  |")
      println("======================================")

      //We load the results obtained after computing the map matching algorithm

      val matchedData = loadResultsData(spark, gps_matched_data)

      //We load the reference data

      val refData = loadRefData(spark, gps_data)

      //We join both RDDs using the object Point as the key

      val joinedData = refData.join(matchedData)

      joinedData.persist()

      //We count the number of points taken into account

      val joinedDataCount = joinedData.count()

      //We compute the correct matches and print the accuracy of the results

      val correctMatches = joinedData
        .filter{case (_, osmID) => osmID._2 == osmID._1}
        .count()

      val accuracy = (correctMatches*10000/joinedDataCount).toDouble/100

      println("% Accuracy: " + accuracy)

      //We take a sample of incorrect matches and write them in a file

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
