package geo.elements

import geo.math.algebra._

import scala.math._

class Point (val lat: Double, val lon: Double, val orientation: Double, val id: String) extends Serializable{

  //if the point has no direction then it is assigned a -1
  def this(x: Double, y:Double) = {

    this(x,y,-1,"")

  }

  //if the point doesn't need an id, then, this variable takes the value or ''
  def this(x: Double, y:Double, dir: Double) = {

    this(x,y,dir,"")

  }

  override def toString: String = "Point(" + lat + "," + lon + ")"

  def toList: List[Double] = {

    //we invert the order of lat, lon because is the format supported by geojson

    List(lon, lat)
  }

  def toGeoVector: GeoVector = {

    val x = cos(lat.toRadians)*cos(lon.toRadians)
    val y = cos(lat.toRadians)*sin(lon.toRadians)
    val z = sin(lat.toRadians)

    new GeoVector(x,y,z)

  }

  def distToPoint (p: Point): Double = {

    haversineFormula(this,p)

  }

  def distToSegment (s: Segment): Double = {

    distToPoint(projectToSegment(s))

  }

  def proximityToSegment (s: Segment): Double = {

    val p = this.toGeoVector
    val a = s.a.toGeoVector
    val b = s.b.toGeoVector

    val v = a x b
    val u = v.normalize

    abs(p*u)

  }

  def projectionInSegment (s: Segment): Boolean = {

    val a = s.a.toGeoVector
    val b = s.b.toGeoVector
    val c = this.toGeoVector

    val u = (a x b).normalize

    val u_a = (u x a).normalize
    val u_b = (b x u).normalize

    if ((c * u_a).signum == 0) false
    else if ((c * u_b).signum == 0) false
    else (c * u_a).signum == (c * u_b).signum

  }

  def projectToSegment (s: Segment): Point = {

    if (projectionInSegment(s)) {

      val d = proximityToSegment(s)

      val x = this.toGeoVector
      val a = s.a.toGeoVector
      val b = s.b.toGeoVector

      val v = a x b
      val u = v.normalize

      val x_p = x - u * d
      val x_pp = x_p.normalize

      x_pp.toPoint

    } else {

      if (distToPoint(s.a) < distToPoint(s.b)) s.a else s.b

    }

  }

  def isPointAligned (s: Segment, t: Double): Boolean = {


    //parameter t is the tolerance in the difference of orientations

    val difference = abs(s.initialBearing-orientation) % 180

    if ((difference <= t ) || (difference >= 180 - t )) true else false

  }

  def orientationDifference (s: Segment): Double = {

    abs(orientation - s.initialBearing)

  }

  def == (p: Point): Boolean = {

    (lat == p.lat) && (lon == p.lon)

  }

}
