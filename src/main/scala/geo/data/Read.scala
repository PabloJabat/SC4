package geo.data

import geo.elements._
import geo.data.Transform._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.io.Source

object Read {

  def loadMapSpark(spark: SparkSession, mapPath: String): RDD[Way] = {

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

  def loadGPSPointsSpark(spark: SparkSession, gpsDataPath: String): RDD[Point] = {

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

  def loadMap(mapPath: String): List[Way] = {

    val osmData = Source.fromFile(mapPath).getLines()

    osmData
      .filter(line => lineHasWay(line))
      .map(line => lineStringToWay(line))
      .toList

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

}
