package geo.algorithms

import geo.elements.{Point, Segment}
import geo.math.algebra.naiveBayesClassifier

object MapMatching {

  def geometricMM (p: Point, segmentsLst: List[(String,Segment)], t: Double = 30.0): ((String,Segment), Point) = {

    val segmentsAligned = segmentsLst.filter{case (_,s) => s.isSegmentAligned(p,t)}

    val bestCandidate = try {

      segmentsAligned
        .minBy{case (_, s) => p.distToSegment(s)}

    } catch {

      case _: UnsupportedOperationException =>

        segmentsLst
          .minBy{case (_, s) => p.distToSegment(s)}

    }

    (bestCandidate, p.projectToSegment(bestCandidate._2))

  }

  def naiveBayesClassifierMM (p: Point, segmentsLst: List[(String,Segment)]): ((String,Segment), Point) = {


    val bestCandidate = segmentsLst
      .minBy{case (_, s) => naiveBayesClassifier(p,s)}

    (bestCandidate, p.projectToSegment(bestCandidate._2))

  }

}
