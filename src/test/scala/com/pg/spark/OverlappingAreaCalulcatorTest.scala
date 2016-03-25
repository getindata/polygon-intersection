package com.pg.spark

import com.pg.SparkContextFactory
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import model.{OverlappingPolygons, Polygon}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._


class OverlappingAreaCalulcatorTest extends FunSuite with Matchers {

  val sc: SparkContext = SparkContextFactory.getSparkContext
  val gf: GeometryFactory = new GeometryFactory()

  val SQUARE: Polygon = makePolygon(42, (0, 0), (2, 0), (2, 2), (0, 2))
  val BIG_SQUARE_WITHOUT_CORNER: Polygon = makePolygon(69, (2, 0), (4, 0), (4, 4), (0, 4), (0, 2))
  val TRIANGLE: Polygon = makePolygon(120, (0, 2), (4, 2), (2, 4))

  test("should calculate correct result for 2 squares intersection") {
    test2Polygons(SQUARE, BIG_SQUARE_WITHOUT_CORNER, 1.0 / 7)
  }

  test("should calculate correct result for 2 squares intersection in reverse order") {
    test2Polygons(BIG_SQUARE_WITHOUT_CORNER, SQUARE, 1.0 / 2)
  }

  test("should calculate correct result for big square and triangle intersection") {
    test2Polygons(BIG_SQUARE_WITHOUT_CORNER, TRIANGLE, 1.0)
  }

  test("should calculate correct result for big square and triangle intersection in reverse order") {
    test2Polygons(TRIANGLE, BIG_SQUARE_WITHOUT_CORNER, 2 * (1.0 / 7))
  }

  test("should calculate correct result for non-intersecting square and triangle") {
    test2Polygons(SQUARE, TRIANGLE, 0.0)
  }

  test("should calculate correct result for non-intersecting square and triangle in reverse order") {
    test2Polygons(TRIANGLE, SQUARE, 0.0)
  }

  test("should calculate multiple results correctly") {
    val input: RDD[(Polygon, Polygon)] = sc.parallelize(List(
      (SQUARE, BIG_SQUARE_WITHOUT_CORNER),
      (TRIANGLE, BIG_SQUARE_WITHOUT_CORNER)
    ))
    val result: Array[OverlappingPolygons] = OverlappingAreaCalculator.calculate(input).collect()

    result.length shouldEqual 2
    result(0) shouldEqual OverlappingPolygons(SQUARE.id, BIG_SQUARE_WITHOUT_CORNER.id, 1.0 / 7)
    result(1) shouldEqual OverlappingPolygons(TRIANGLE.id, BIG_SQUARE_WITHOUT_CORNER.id, 2 *  (1.0 / 7))
  }

  test("should work for empty input") {
    val input: RDD[(Polygon, Polygon)] = sc.parallelize(List())

    val result: Array[OverlappingPolygons] = OverlappingAreaCalculator.calculate(input).collect()

    result.length shouldEqual 0
  }

  private def test2Polygons(first: Polygon, second: Polygon, expectedFraction: Double): Unit = {
    val input: RDD[(Polygon, Polygon)] = sc.parallelize(List((first, second)))
    val result: Array[OverlappingPolygons] = OverlappingAreaCalculator.calculate(input).collect()

    result.length shouldEqual 1
    result(0) shouldEqual OverlappingPolygons(first.id, second.id, expectedFraction)
  }

  private def makePolygon(id: Long, coordinates: (Long, Long)*): Polygon = {
    val first = Array(new Coordinate(coordinates.head._1, coordinates.head._2))
    val array: Array[Coordinate] = coordinates.map { case (x, y) => new Coordinate(x, y) }.toArray
    Polygon(id, gf.createPolygon(array ++ first))
  }
}
