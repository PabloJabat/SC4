package geo.elements


class Way (val points: List[Point], val osmID: String) extends Serializable{

  override def toString: String = "Way@" + osmID

  def toSegmentsList: List[Segment] = {

    points
      .sliding(2,1)
      .map{case List(a,b) => new Segment(a,b,osmID)}
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

  def distToPoint(p: Point, t: Double = 30.0): Double = {

    try {

      points
        .sliding(2,1)
        .map(lstPoints => new Segment(lstPoints.head,lstPoints(1)))
        .filter(s => s.isSegmentAligned(p,t))
        .map(s => p.projectToSegment(s))
        .map(a => a.distToPoint(p))
        .min

    } catch {

      case _: UnsupportedOperationException =>

        distToPoint(p)

    }

  }

  def toListList: List[List[Double]] = {

    points.map(a => a.toList)

  }

  def osmBoxSize: (Double, Double) = {

    val latDiff = points.map(_.lat).max - points.map(_.lat).min
    val lonDiff = points.map(_.lon).max - points.map(_.lon).min

    (latDiff, lonDiff)

  }

}
