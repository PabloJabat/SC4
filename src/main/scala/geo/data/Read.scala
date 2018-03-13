package geo.data

import geo.elements.{Grid, Point, Way}
import geo.data.Transform.{findWayID, lineStringToPointArray, stringToPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Read {

  def loadMap(spark: SparkSession, mapPath: String): RDD[Way] = {

    import java.net.{URI => JavaURI}
    import net.sansa_stack.rdf.spark.io.NTripleReader
    import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}

    val sc = spark.sparkContext
    val ops = JenaSparkRDDOps(sc)

    import ops._

    val mapData: TripleRDD = NTripleReader.load(spark, JavaURI.create(mapPath))

    mapData
      .find(ANY,URI("http://www.opengis.net/ont/geosparql#asWKT"),ANY)
      .map(a => (findWayID(a.getSubject.toString),a.getObject.toString))
      .map{case (id, way) => new Way(lineStringToPointArray(way).toList,id)}

  }

  def loadGPSPoints(spark: SparkSession, gpsDataPath: String): RDD[Point] = {

    val pattern = """([^";]+)""".r

    spark.sparkContext.textFile(gpsDataPath)
      .map(line => pattern.findAllIn(line).toList)
      .map(pointExtraction)

  }

  def loadRefData(spark: SparkSession, refGPSDataPath: String): RDD[(Point, String)] = {

    val sc = spark.sparkContext

    sc.textFile(refGPSDataPath)
      .map(_.split(",").toList)
      .map(referenceGPSDataExtraction)

  }

  def loadResultsData(spark: SparkSession, matchedGPSDataPath: String): RDD[(Point, String)] = {

    val sc = spark.sparkContext

    sc.textFile(matchedGPSDataPath)
      .map(_.split(",").toList)
      .map(matchedGPSDataExtraction)

  }

  def filterIndexMap(rddWays: RDD[Way], grid: Grid): RDD[(String, List[Way])] = {

    rddWays
      .filter(w => grid.hasWay(w))
      .map(w => (grid.indexWay(w),w))
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}
      .groupByKey
      .mapValues(_.toList)

  }

  def filterIndexGPSPoints(rddGPSPoints: RDD[Point], grid: Grid): RDD[(String, Point)] = {

    rddGPSPoints
      .filter(p => grid.clearanceBoxHasPoint(p))
      .map(p => (grid.indexPoint(p),p))
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}

  }

  def joinIndexedMapPoints(rddGPSPoints: RDD[(String, Point)], rddWays: RDD[(String, List[Way])]): RDD[(Point, List[Way])] = {

    rddGPSPoints.join(rddWays)
      .values
      .groupByKey
      .flatMapValues(_.toList)
     // .mapValues(_.flatten.toList)

  }


  private def pointExtraction(list: List[String]): Point = {
    //We first put the 4th entry as it is the latitude and we want the LatLon array
    new Point(list(3).toDouble, list(2).toDouble, list(6).toDouble, list.head + " " + list(1))

  }

  private def matchedGPSDataExtraction(list: List[String]): (Point, String) = {

    val point = new Point(list(2).toDouble, list(3).toDouble, list(1))

    (point, list.head)

  }

  private def referenceGPSDataExtraction(list: List[String]): (Point, String) = {

    val pattern = "([^{}]+)".r
    val id = list(1) + " " + list(2)
    val point = new Point(list(4).toDouble, list(3).toDouble, id)

    (point, pattern.findFirstIn(list(9)).get)

  }

  private def matchedGPSDataExtractionStats(list: List[String]): (Point, String, Double, Point) = {

    val pattern1 = "[0-9]+".r
    val pattern2 = "[0-9]+.[0-9]+".r

    (new Point(list(4).toDouble,list(3).toDouble, list(7).toDouble),
      pattern1.findFirstIn(list(9)).get,
      pattern2.findFirstIn(list(9)).get.toDouble,
      stringToPoint(list(11)))

  }

}
