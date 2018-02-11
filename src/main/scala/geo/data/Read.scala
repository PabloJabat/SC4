package geo.data

import geo.elements.Point

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

}
