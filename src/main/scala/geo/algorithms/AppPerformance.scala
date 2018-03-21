package geo.algorithms

import org.apache.spark.sql.SparkSession
import geo.data.Read._
import java.io._

import geo.algorithms.MapMatching._
import geo.data.Transform._
import geo.data.Write._
import geo.elements.{BoxLimits, Grid}

object AppPerformance {

  def main(args: Array[String]) {

    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in1, config.in2, config.in3)
      case None =>
        println(parser.usage)

    }

    def run (gps_matched_data: String, gps_data: String, osm_data: String): Unit = {

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
        .filter{case (_, osmID) => osmID._1._1 == osmID._2._1}
        .count()

      val accuracy = (correctMatches*10000/joinedDataCount).toDouble/100

      println("% Accuracy: " + accuracy)

      //We take a sample of incorrect matches and write them in a file

      val incorrectMatches = joinedData
        .filter{case (_, osmID) => !(osmID._1._1 == osmID._2._1)}

      val pw1 = new PrintWriter("/home/pablo/DE/DataSets/incorrectMatches.txt")

      val someIncorrectMatches = incorrectMatches
        .takeSample(withReplacement = false, 10)
        .map {case (p, points) => (p,(points._1._2,points._2._2))}

      pw1.close()

      //We create the same grid used in the MM algorithm

      val osmBox = BoxLimits(40.65, 40.64, 22.94, 22.93)

      val myGrid = new Grid(osmBox,150,0)

      //We index the points that we want to draw and convert the data to an RDD[(String, Point)]

      val pointsData = someIncorrectMatches
        .map(a => a._1)
        .map(p => (myGrid.indexPoint(p),p))
        .flatMap{case (k,v) => for (i <- k) yield (i, v)}

      val pointsIndexed = spark.sparkContext.parallelize(pointsData)

      //We load the osm data and index the ways

      val osmMap = loadMapSpark(spark,osm_data)

      val waysIndexedFiltered = filterIndexMapSpark(osmMap, myGrid)

      //We join the points and ways

      val pointWays = joinIndexedMapPointsSpark(pointsIndexed, waysIndexedFiltered)

      //We transform someIncorrectMatches into an RDD called someIncorrectMatchesRDD

      val someIncorrectMatchesRDD = spark.sparkContext.parallelize(someIncorrectMatches)

      //We joint someIncorrectMatchesRDD and pointWays to obtain all the data needed to draw the results

      val mergedData = pointWays.join(someIncorrectMatchesRDD).map{case (p,(ways,osmIDs)) => (p,ways,osmIDs)}.collect().toList

      //incorrectMatchesToJSON which transforms the data to .json and saves it

      val pw2 = new PrintWriter("/home/pablo/GeoJSON/incorrectMatchesGeoJSON.json")

      incorrectMatchesToJSON(pw2,mergedData,myGrid)

    }

  }

  case class Config(in1: String = "", in2: String = "", in3: String = "")

  val parser: scopt.OptionParser[Config] = new scopt.OptionParser[Config]("Simple Map Matching Performance") {

    head("Simple Map Matching Performance")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in1 = x)).
      text("path to file that contains the gps data (in .csv format)")

    opt[String]('o', "output").required().valueName("<path>").
      action((x, c) => c.copy(in2 = x)).
      text("path to file that contains the gps matched data (in .csv format)")

    opt[String]('m', "map").required().valueName("<path>").
      action((x, c) => c.copy(in3 = x)).
      text("path to file that contains the map data (in .nt format)")

    help("help").text("prints this usage text")

  }

}
