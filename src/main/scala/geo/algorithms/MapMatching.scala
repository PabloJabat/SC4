package geo.algorithms

import geo.elements.{Point, Segment, Way}
import geo.math.algebra.naiveBayesClassifier

object MapMatching {

  def geometricMM (p: Point, segmentsLst: List[Segment], t: Double = 30.0): (Segment, Point) = {

    val segmentsAligned = segmentsLst.filter(s => s.isSegmentAligned(p,t))

    val bestCandidate = try {

      segmentsAligned
        .minBy( s => p.distToSegment(s) )

    } catch {

      case _: UnsupportedOperationException =>

        segmentsLst
          .minBy( s => p.distToSegment(s) )

    }

    (bestCandidate, p.projectToSegment(bestCandidate))

  }

  def geometricMM2 (p: Point, waysLst: List[Way], t: Double = 30.0): (Way, Point) = {

    val segmentsLst = waysLst.flatMap(w => w.toSegmentsList)

    val (bestSegment, projection) = geometricMM(p,segmentsLst,t)

    val bestWay = waysLst.filter(w => bestSegment.osmID == w.osmID).head

    (bestWay, projection)

  }

  def naiveBayesClassifierMM2 (p: Point, segmentsLst: List[(String,Segment)], stdev_b: Double, stdev_deltaPhi: Double): ((String,Segment), Point) = {

    val bestCandidate = segmentsLst
      .minBy{case (_, s) => naiveBayesClassifier(p,s,stdev_b,stdev_deltaPhi)}

    (bestCandidate, p.projectToSegment(bestCandidate._2))

  }

  def naiveBayesClassifierMM (p: Point, waysLst: List[Way], stdev_b: Double, stdev_deltaPhi: Double): (Way, Point) = {

    val bestCandidate = waysLst
      .minBy(w => naiveBayesClassifier(p,w,stdev_b,stdev_deltaPhi))

    val bestSegmentOfBestCandidate = bestCandidate.toSegmentsList
      .minBy(s => naiveBayesClassifier(p,s,stdev_b,stdev_deltaPhi))

    (bestCandidate, p.projectToSegment(bestSegmentOfBestCandidate))

  }

}
