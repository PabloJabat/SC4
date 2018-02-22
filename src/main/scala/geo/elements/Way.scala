package geo.elements

import net.liftweb.json.DefaultFormats

import net.liftweb.json.Serialization.write

class Way (val points: List[Point]) extends Serializable{

  def toSegmentsList: List[Segment] = {

    points
      .sliding(2,1)
      .map{case List(a,b) => new Segment(a,b)}
      .toList

  }

  def distToPoint(p: Point): Double = {

    points
      .sliding(2,1)
      .map(a => new Segment(a.head,a(1)))
      .map(s => p.projectToSegment(s))
      .map(a => a.distToPoint(p))
      .min

  }

  def toJSON: String = {

    implicit val formats = DefaultFormats

    case class Empty()
    case class Geometry(geometry: Any,`type`: String = "Feature", properties: Any = Empty())
    case class WayGeoJSON(coordinates: List[List[Double]],`type`: String = "LineString")

    write(Geometry(WayGeoJSON(toListList)))

  }

  def toListList: List[List[Double]] = {

    points.map(a => a.toList)

  }

}
