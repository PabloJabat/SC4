package geo.algorithms

import java.io.PrintWriter
import geo.elements._
import geo.data.Read._
import geo.data.Transform._
import geo.data.Write._
import geo.algorithms.MapMatching._

object AppTest {

  def main(args: Array[String]): Unit = {

    //We create a function that will be used to check if at least one of the elements appears in the lst variable
    def someElementsInList(lst: List[String], elements: List[String]): Boolean = {

      val comparison = elements.map(e => lst.contains(e))

      comparison.reduce(_ || _)

    }

    val p = new Point(40.637, 22.953, 315)

    //We create the grid that we are going to use
    val osmBox = BoxLimits(40.638, 40.63, 22.96, 22.9450)

    val myGrid = new Grid(osmBox,80)

    //We load the map with to a List[Way]
    val waysData = loadMap(args(0))

    //We index the ways using the grid we just created
    val waysDataIndexed = filterIndexMap(waysData, myGrid)

    //We get the indexes of our point and print them
    val pointIndexes = myGrid.indexPoint(p).distinct

    println(pointIndexes)

    //We filter the necessary ways
    val waysOfPoint = getWaysOfIndexes(waysDataIndexed, pointIndexes)

    //We write to geojson all the ways that will we introduced in the MM algorithm
    val waysToJSON = getIndexedWaysOfIndexes(waysDataIndexed, pointIndexes)

    val pw1 = new PrintWriter("/home/pablo/GeoJSON/CellWays.json")

    cellsToJSON(pw1, waysToJSON, myGrid)

    //We call the map matching algorithm
    val result = geometricMM2(p, waysOfPoint)

    //We write the result to geojson
    val pw2 = new PrintWriter("/home/pablo/GeoJSON/MMResult.json")

    resultsToJSON(pw2, List((p, result._2, waysOfPoint)), myGrid)

  }

}
