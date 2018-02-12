package geo.math

import geo.elements.{Point, Segment}

import scala.math._

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

  def haversineFormula (a: Point, b: Point): Double = {

    val R = 6371e3

    val sigma1 = a.x.toRadians
    val sigma2 = b.x.toRadians
    val sigmaDelta = (b.x-a.x).toRadians
    val lambdaDelta = (b.y-a.y).toRadians

    val A = sin(sigmaDelta/2)*sin(sigmaDelta/2) + cos(sigma1)*cos(sigma2)*sin(lambdaDelta/2)*sin(lambdaDelta/2)
    val C = 2*atan2(sqrt(A), sqrt(1-A))

    R * C

  }
}
