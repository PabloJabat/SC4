package geo.math

import geo.elements.{Point, Segment}

import scala.math._

object algebra {

  def haversineFormula (a: Point, b: Point): Double = {

    val R = 6371e3

    val sigma1 = a.lat.toRadians
    val sigma2 = b.lat.toRadians
    val sigmaDelta = (b.lat-a.lat).toRadians
    val lambdaDelta = (b.lon-a.lon).toRadians

    val A = sin(sigmaDelta/2)*sin(sigmaDelta/2) + cos(sigma1)*cos(sigma2)*sin(lambdaDelta/2)*sin(lambdaDelta/2)
    val C = 2*atan2(sqrt(A), sqrt(1-A))

    R * C

  }

  def naiveBayesClassifier (p: Point, s: Segment, stdev_b: Double = 5.0, stdev_deltaPhi: Double = 1000000.0): Double = {

    val b = p.distToSegment(s)
    val deltaPhi = p.orientationDifference(s)

    pow(b,2)/stdev_b + pow(deltaPhi,2)/stdev_deltaPhi

  }

}
