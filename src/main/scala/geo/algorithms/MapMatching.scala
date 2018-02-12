package geo.algorithms

import geo.elements.{Point, Segment}

object MapMatching {

  def pointToLine (p: Point, ss: List[(String,Segment)]): (String, Point) = {

    var ssList: List[(String, Segment)] = List()

    val ssFiltered = ss.filter{case (id,s) => s.isSegmentAligned(p)}

    if (ssFiltered.length < 3) ssList = ss else ssList = ssFiltered
    val bestCandidate = ssList
      .map{case (id, s) => (id, p.distToSegment(s), s, p.isPointAligned(s))}
      .reduce((a, b) =>
        if (a._2 < b._2)  a else b)

    val bestCandidates = ssList.groupBy{case (id, s) => p.distToSegment(s)}.toList.min
    bestCandidates
    (bestCandidate._1, p.projectToSegment(bestCandidate._3))

  }

}
