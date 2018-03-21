package geo.algorithms

import java.io._
import org.apache.spark.sql._

import geo.elements._
import geo.data.Read._
import geo.data.Transform._
import geo.data.Write._
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
//      .config("spark.jars", jars)
      .getOrCreate()

    import spark.implicits._

    println("======================================")
    println("|         Simple Map Matching        |")
    println("======================================")

    //We first set the region on the map in which we want to perform map matching

    val osmBox = BoxLimits(40.65, 40.64, 22.94, 22.93)

    val myGrid = new Grid(osmBox,150,0)

    println("Number of latitude divisions: " + myGrid.latDivisions)

    println("Number of longitude divisions: " + myGrid.lonDivisions)

    //We load and index the GPS data

    val gpsData = loadGPSPointsSpark(spark, gps_data)

    val gpsDataIndexed = filterIndexGPSPointsSpark(gpsData, myGrid)

    //We load and index and group the Ways data

    val waysData = loadMapSpark(spark, osm_data)

    val waysDataIndexed = filterIndexMapSpark(waysData, myGrid)

    //Now we are going to write a file with some sample data of ways already indexed

    waysDataIndexed.persist()

    val cells = waysDataIndexed.take(10).toList

    val pw1 = new PrintWriter("/home/pablo/GeoJSON/cellWaysRDD.json")

    cellsToJSON(pw1,cells,myGrid)

    waysDataIndexed.unpersist()

    //The last step is to join both RDDs

    val mergedData = joinIndexedMapPointsSpark(gpsDataIndexed, waysDataIndexed)

    //Now we want to visualize some of the inputs for the Map Matching algorithm

    mergedData.persist()

    val sample = mergedData.take(10).toList

    val pw2 = new PrintWriter("/home/pablo/GeoJSON/mergedDataRDD.json")

   indexedDataToJSON(pw2,sample,myGrid)

    mergedData.unpersist()

    //The last step is to pass the resulting RDD to the map matching algorithm

    val matchedData = mergedData
      .map{case (p: Point, waysLst: List[Way]) => (geometricMM2(p,waysLst,90),p,waysLst)}
      .map{case ((way, new_p), p, ways) => (way, p, new_p, ways)}

    //Once we carry out the Map Matching computation we write the results in a csv

    matchedData.persist()

    matchedData.map{case (way, p, new_p, _) => (way.osmID, p.id, p.lat, p.lon, p.orientation, new_p.lat, new_p.lon)}
      .toDF("wayID","pointID","latitude","longitude","orientation","matched latitude","matched longitude")
      .coalesce(1).write.csv("/home/pablo/results")

    matchedData.unpersist()

    //We can also write some results in geojson format for visualization purposes

    val someResults = matchedData.take(20).toList

    val pw3 = new PrintWriter("/home/pablo/GeoJSON/MMResultsRDD.json")

    val jsonResults = someResults
      .map(a => (a._2, a._3, a._4))

    resultsToJSON(pw3, jsonResults, myGrid)

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
