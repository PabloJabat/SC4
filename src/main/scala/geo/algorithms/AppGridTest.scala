package geo.algorithms

import java.io.PrintWriter
import geo.data.Transform._
import geo.elements.{BoxLimits, Grid}
import geo.elements.Point
import geo.data.Write._

import scala.io.Source

object AppGridTest {

  def main(args: Array[String]): Unit = {

    def allElementsInList(lst: List[String], elements: List[String]): Boolean = {

      val comparison = elements.map(e => lst.contains(e))

      comparison.reduce(_ && _)

    }

    def someElementsInList(lst: List[String], elements: List[String]): Boolean = {

      val comparison = elements.map(e => lst.contains(e))

      comparison.reduce(_ || _)

    }

    val p = new Point(40.637, 22.953, 315)

    val osmData = Source.fromFile(args(0)).getLines()

    val waysData = osmData
      .filter(line => lineHasWay(line))
      .map(line => lineStringtoWay(line))
      .toList

    val osmBox = BoxLimits(40.638, 40.63, 22.96, 22.9450)
    val myGrid = new Grid(osmBox,80)

    val waysDataIndexed = waysData
      .filter(w => myGrid.hasWay(w))
      .map(w => (myGrid.indexWay(w), w))
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}
      .groupBy{case (k,v) => k}
      .map(a => (a._1, a._2.map(b => b._2)))
      .toList


    val pointIndexes = myGrid.indexPoint(p,15).distinct

    println(pointIndexes)

    val waysOfPoint = waysDataIndexed
      .filter{case (k,v) => pointIndexes.contains(k)}
      .flatMap(a => a._2)

//    val otherWays = waysDataIndexed
//      .filter{case (k,v) => !pointIndexes.contains(k)}
//      .flatMap(a => a._2)

    val otherWays = waysData
      .filter(w => myGrid.hasWay(w))
      .map(w => (myGrid.indexWay(w), w))
      .filter{case (indexes, _) => !someElementsInList(indexes,pointIndexes)}
      .flatMap{case (k,v) => for (i <- k) yield (i, v)}
      .groupBy{case (k,v) => k}
      .map(a => (a._1, a._2.map(b => b._2)))
      .toList
      .flatMap(a => a._2)

    val pw1 = new PrintWriter("/home/pablo/gridways.json")
    val pw2 = new PrintWriter("/home/pablo/othergridways.json")

    pointWaysToJSON(pw1,waysOfPoint,p,pointIndexes,myGrid)
    otherWaysToJSON(pw2,otherWays)

    println(waysOfPoint.length)

  }

}
