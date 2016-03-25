package com.pg.spark

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import model.Polygon

trait PolygonFixtures {

  private val gf: GeometryFactory = new GeometryFactory()

  val SQUARE: Polygon = makePolygon(42, (0, 0), (2, 0), (2, 2), (0, 2))
  val BIG_SQUARE_WITHOUT_CORNER: Polygon = makePolygon(69, (2, 0), (4, 0), (4, 4), (0, 4), (0, 2))
  val TRIANGLE: Polygon = makePolygon(120, (0, 2), (4, 2), (2, 4))
  val RECTANGLE: Polygon = makePolygon(1024, (-2, -1), (-1, -1), (-1, -3), (-2, -3))

  private def makePolygon(id: Long, coordinates: (Long, Long)*): Polygon = {
    val first = Array(new Coordinate(coordinates.head._1, coordinates.head._2))
    val array: Array[Coordinate] = coordinates.map { case (x, y) => new Coordinate(x, y) }.toArray
    Polygon(id, gf.createPolygon(array ++ first))
  }
}
