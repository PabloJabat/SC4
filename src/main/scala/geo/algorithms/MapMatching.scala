package geo.algorithms

import geo.elements.{Point, Segment}

object MapMatching {

  def pointToLine (p: Point, ss: List[(String,Segment)]): (String, Point) = {

    val bestCandidate = ss
      .map{case (id, s) => (id, p.distToSegment(s), s)}
      .reduce{case ((a_id ,a_d, a_s), (b_id ,b_d, b_s)) => if (a_d < b_d) (a_id ,a_d, a_s) else (b_id ,b_d, b_s)}

    (bestCandidate._1, p.projectToSegment(bestCandidate._3))

  }

}
