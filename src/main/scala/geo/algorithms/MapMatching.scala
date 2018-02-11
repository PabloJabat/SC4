package geo.algorithms

import geo.elements.{Point, Segment}

object MapMatching {

  def pointToLine (p: Point, ss: List[(String,Segment)]): (List[String], Point) = {

    var ssList: List[(String, Segment)] = List()
    val ssFiltered = ss.filter{case (_,s) => s.isSegmentAligned(p)}

    if (ssFiltered.lengthCompare(3) < 0) ssList = ss else ssList = ssFiltered

    val bestCandidates = ssList
      .groupBy{case (_, s) => p.distToSegment(s)}
      .toList
      .minBy{case (dist, _) => dist}

    (bestCandidates._2.map(a => a._1), p.projectToSegment(bestCandidates._2.head._2))

  }

}
