package geo.elements

class Way (val points: List[Point]) extends Serializable{

  def distToPoint(p: Point): Double = {

    points
      .sliding(2,1)
      .map(a => new Segment(a.head,a(1)))
      .map(s => p.projectToSegment(s))
      .map(a => a.distToPoint(p))
      .min

  }

}
