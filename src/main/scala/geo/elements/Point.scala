package geo.elements

import geo.math.algebra.{area, height, outOfSegment}

import scala.math.{min, pow, sqrt}

class Point (val x: Double, val y: Double) extends Serializable{

  //The x is the Latitude and y is the Longitude
  override def toString: String = "Point(" + x + "," + y + ")"

  def distToPoint (p: Point): Double = {

    sqrt(pow(x - p.x, 2) + pow(y - p.y, 2))

  }

  def distToSegment (s: Segment, inf: Boolean = false): Double = {

    if (inf) {

      if (outOfSegment(this, s))
        min(s.a.distToPoint(this),s.b.distToPoint(this))
      else
        area(s.a, s.b, this) * 2 / s.norm

    } else area(s.a, s.b, this) * 2 / s.norm

  }

  def - (p: Point): (Double,Double) ={

    (x - p.x, y - p.y)

  }

  def projectToSegment (s: Segment): Point = {

    if (outOfSegment(this, s))
    {
      if (this.distToPoint(s.a) > this.distToPoint(s.b)) s.b else s.a
    } else {
      val bigHyp = s.norm
      val smallHyp = sqrt(pow(this.distToPoint(s.a),2)-pow(this.distToSegment(s,inf = true),2))
      val (diffLat, diffLon) = s.a - s.b
      val incrLat = smallHyp * diffLat / bigHyp
      val incrLon = smallHyp * diffLon / bigHyp
      new Point(x + incrLat, y + incrLon)
    }

  }

}
