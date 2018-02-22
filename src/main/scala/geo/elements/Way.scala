package geo.elements


class Way (val points: List[Point]) extends Serializable{

  def toSegmentsList: List[Segment] = {

    points
      .sliding(2,1)
      .map{case List(a,b) => new Segment(a,b)}
      .toList

  }

  def distToPoint(p: Point): Double = {

    points
      .sliding(2,1)
      .map(a => new Segment(a.head,a(1)))
      .map(s => p.projectToSegment(s))
      .map(a => a.distToPoint(p))
      .min

  }

  def toListList: List[List[Double]] = {

    points.map(a => a.toList)

  }

}
