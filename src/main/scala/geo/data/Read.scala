package geo.data

import geo.elements.Point
import geo.data.Transform.stringToPoint

object Read {

  def dataExtraction(list: List[String]): Point = {
    //We first put the 4th entry as it is the latitude and we want the LatLon array
    new Point(list(3).toDouble, list(2).toDouble, list(6).toDouble, list.head + " " + list(1))

  }

  def matchedGPSDataExtraction(list: List[String]): (String, List[String]) = {

    (list(1), list.head.split("-").toList)

  }

  def referenceGPSDataExtraction(list: List[String]): (String, String) = {

    val pattern = "([^{}]+)".r
    (list(1) + " " + list(2), pattern.findFirstIn(list(9)).get)

  }

  def matchedGPSDataExtractionStats(list: List[String]): (Point, String, Double, Point) = {

    val pattern1 = "[0-9]+".r
    val pattern2 = "[0-9]+.[0-9]+".r

    (new Point(list(4).toDouble,list(3).toDouble, list(7).toDouble),
      pattern1.findFirstIn(list(9)).get,
      pattern2.findFirstIn(list(9)).get.toDouble,
      stringToPoint(list(11)))

  }

}
