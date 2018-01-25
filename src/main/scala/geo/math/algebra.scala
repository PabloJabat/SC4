package geo.math

import geo.elements.{Point, Segment}

import scala.math.{abs,pow,acos,Pi}

object algebra {

  def area(a: Point, b: Point, c: Point): Double = {
    abs(a.x * (b.y - c.y) + b.x * (c.y - a.y) + c.x * (a.y - b.y)) / 2
  }

  def height(p: Point, segment: Segment): Double = {
    area(segment.a, segment.b, p) * 2 / segment.norm
  }

  def outOfSegment(p: Point, s: Segment): Boolean = {

    val a = s.norm
    val b = p.distToPoint(s.b)
    val c = p.distToPoint(s.a)

    val beta = acos((pow(a,2)+pow(c,2)-pow(b,2))/(2*a*c))
    val gamma = acos((pow(a,2)+pow(b,2)-pow(c,2))/(2*a*b))

    if (beta > Pi/2 || gamma > Pi/2) true else false
  }

}
