package geo.elements


import geo.math.algebra.{area, outOfSegment}

import scala.math.{min, pow, sqrt}

class Segment (val a: Point, val b: Point) extends Serializable{

  def norm: Double ={

    sqrt(pow(a.x - b.x, 2) + pow(a.y - b.y, 2))

  }

  def distToPoint (p: Point): Double = {

    if (outOfSegment(p, this))
      min(a.distToPoint(p),b.distToPoint(p))
    else
      area(a, b, p) * 2 / this.norm

  }

}
