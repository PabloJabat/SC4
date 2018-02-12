package geo.elements


import geo.math.algebra.{area, outOfSegment}

import scala.math._

class Segment (val a: Point, val b: Point) extends Serializable{

  // a has to be the starting point of the segment and b the ending point
  // dir is the angle towards the vertex b. 0 degrees is North, 90 degrees is East, etc.

  val dir: Double = 90-toDegrees(atan2(b.x-a.x,b.y-a.y))

  def norm: Double ={

    sqrt(pow(a.x - b.x, 2) + pow(a.y - b.y, 2))

  }

  def isSegmentAligned (p: Point): Boolean = {

    val difference = abs(p.dir-dir)

    if (difference<=10) true
    else if (difference>=170 && difference <=190)
      true
    else if (difference>=350)
      true
    else
      false

  }

  def distToPoint (p: Point): Double = {

    if (outOfSegment(p, this))
      min(a.distToPoint(p),b.distToPoint(p))
    else
      area(a, b, p) * 2 / this.norm

  }

}
