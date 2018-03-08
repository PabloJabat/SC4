package geo.algorithms

import java.net.{URI => JavaURI}

import java.io._
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}
import org.apache.spark.sql._

import geo.elements._
import geo.data.Transform._
import geo.data.Read._
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

    val sc = spark.sparkContext
    val ops = JenaSparkRDDOps(sc)
    import ops._

    //We need it if we want to transform tuple RDD to DF
    import spark.implicits._

    println("======================================")
    println("|         Simple Map Matching        |")
    println("======================================")

    val pattern = """([^";]+)""".r

    // We first set the region on the map in which we want to perform map matching
    val osmBox = BoxLimits(40.6486, 40.6374, 22.9478, 22.9298)
    val myGrid = new Grid(osmBox,500)

    println("Number of latitude divisions: " + myGrid.latDivisions)
    println("Number of longitude divisions: " + myGrid.lonDivisions)


    val gpsData = sc.textFile(gps_data)
      .map(line => pattern.findAllIn(line).toList)
      .map(dataExtraction)

    gpsData.persist()

    println("Points before filtering: " + gpsData.count())

    val gpsDataFiltered = gpsData
      .filter(p => myGrid.clearanceBoxHasPoint(p))

    gpsDataFiltered.persist()
    gpsData.unpersist()

    println("Points after filtering: " + gpsDataFiltered.count())

    //We now load the open street map data and collect the information about ways
    val mapData: TripleRDD = NTripleReader.load(spark, JavaURI.create(osm_data))

    val waysTriples = mapData
      .find(ANY,URI("http://www.opengis.net/ont/geosparql#asWKT"),ANY)

    val waysData = waysTriples
      .map(a => (findWayID(a.getSubject.toString),a.getObject.toString))
      .map{case (id, way) => new Way(lineStringToPointArray(way).toList,id)}

    waysData.persist()

    println("Ways before filtering: " + waysData.count())

    val waysDataFiltered = waysData
      .filter(w => myGrid.hasWay(w))

    waysDataFiltered.persist()

    println("Ways after filtering: " + waysDataFiltered.count())

    val gpsDataIndexed = gpsDataFiltered
      .map(p => (myGrid.indexPoint(p),p))

    gpsDataIndexed.persist()

    println("Points before Flattening: " + gpsDataIndexed.count())

    val gpsDataIndexedFlatted = gpsDataIndexed
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}

    gpsDataIndexed.unpersist()
    gpsDataIndexedFlatted.persist()

    println("Points after Flattening: " + gpsDataIndexedFlatted.count())

    val waysDataIndexed = waysDataFiltered
      .map(w => (myGrid.indexWay(w),w))
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}

    waysDataFiltered.unpersist()
    waysDataIndexed.persist()

    println("Ways before Grouping: " + waysDataIndexed.count())

    val waysDataIndexedGrouped = waysDataIndexed
      .groupByKey
      .mapValues(_.toList)

    waysDataIndexedGrouped.persist()
    waysDataIndexed.unpersist()

    println("Ways after Indexing and Grouping: " + waysDataIndexedGrouped.count())

    val cells = waysDataIndexedGrouped.take(10).toList
    val pw = new PrintWriter("/home/pablo/cellWaysRDD.json")

    cellsWaysToJSON(pw,cells,myGrid)

    val mergedData = gpsDataIndexedFlatted
      .join(waysDataIndexedGrouped)

    mergedData.persist()

    val mergedDataV = mergedData.map{case (index,(p, ways)) => (p, (index, ways))}

    println("MergedData: " + mergedData.count())

    val mergedDataGroupedV = mergedDataV
      .groupByKey
      .mapValues(_.toList)

    val mergedDataGrouped = mergedData
      .values
      .groupByKey
      .mapValues(_.flatten.toList)

    mergedData.unpersist()

    mergedDataGroupedV.persist()

    println("MergedDataGrouped: " + mergedDataGroupedV.count())

    val sample = mergedDataGroupedV.take(10).toList

    mergedDataGroupedV.unpersist()

    val pw2 = new PrintWriter("/home/pablo/mergedDataRDD.json")
    mergedDataToJSON(pw2,sample,myGrid)

    waysDataIndexedGrouped.unpersist()
    gpsDataIndexedFlatted.unpersist()

    val matchedData = mergedDataGrouped
      .map{case (p: Point, waysLst: List[Way]) => (geometricMM(p,waysLst.flatMap(_.toSegmentsList),90),p,waysLst)}
      .map{case ((way, new_p), p, ways) => (way, p, new_p, ways)}

    matchedData.map{case (way, p, new_p, _) => (way.osmID, p.id, p.lat, p.lon, new_p.lat, new_p.lon)}
      .toDF("waysID","pointID","latitude","longitude","matched latitude","matched longitude")
      .coalesce(1).write.csv("/home/pablo/results")


//    val someResults =  matchedData.take(1)
//      .map{case ((w, m_p),p,lstW) => (w,p,m_p,lstW)}
//
//    println(someResults.length)
//
//    val pw = new PrintWriter("/home/pablo/geojsonResults.json")
//    val jsonResults = someResults
//      .map(a => (a._2,a._3,a._4)).toList
//
//    println(jsonResults.head._3.size)
//
//    resultsToJSON(pw, jsonResults)
//
//    someResults.foreach{
//
//      case (id, p, new_p, _) =>
//
//        println("Id of way: " + id.toString)
//        println("Map Matched Point: " + new_p.toString)
//        println("Original Point: " + p.toString)

//    }
//
//    someResults.foreach{
//
//      case (w, p, _, _) =>
//        println(pointToJSON(p))
//        println(wayToJSON(w))
//
//    }

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
