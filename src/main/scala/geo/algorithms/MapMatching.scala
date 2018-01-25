package geo.algorithms

import geo.elements.{Point, Segment}

object MapMatching {

  def pointToLine (p: Point, ss: List[Segment]): Point = {

    val bestCandidate = ss.map(s => (s, p.distToSegment(s))).reduce((a,b) => if (a._2 < b._2) a else b)
    p.projectToSegment(bestCandidate._1)

  }

}
