package geo.data

import geo.elements.Point

object Read {

  def dataExtraction(list: Array[String]): Point = {
    //We first put the 4th entry as it is the latitude and we want the LatLon array
    new Point(list(4).toDouble, list(3).toDouble)
  }

}
