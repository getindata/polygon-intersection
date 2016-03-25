package com.pg.spark

import com.pg.SparkContextFactory
import model.Polygon
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{FunSuite, Matchers}

class SelfIntersectionsCalculatorTest extends FunSuite with Matchers with PolygonFixtures {

  val sc: SparkContext = SparkContextFactory.getSparkContext

  test("should calculate intersections for 2 polygons with touching borders") {
    testIntersection(
      List(SQUARE, TRIANGLE),
      List(
        (SQUARE, SQUARE),
        (TRIANGLE, TRIANGLE),
        (SQUARE, TRIANGLE),
        (TRIANGLE, SQUARE)
      ))
  }

  test("should calculate correct result for 2 non-intersecting polygons") {
    testIntersection(
      List(SQUARE, RECTANGLE),
      List(
        (SQUARE, SQUARE),
        (RECTANGLE, RECTANGLE)
      ))
  }

  test("should calculate intersections for 3 polygons") {
    testIntersection(
      List(SQUARE, RECTANGLE, BIG_SQUARE_WITHOUT_CORNER),
      List(
        (SQUARE, SQUARE),
        (RECTANGLE, RECTANGLE),
        (BIG_SQUARE_WITHOUT_CORNER, BIG_SQUARE_WITHOUT_CORNER),
        (SQUARE, BIG_SQUARE_WITHOUT_CORNER),
        (BIG_SQUARE_WITHOUT_CORNER, SQUARE)
      ))
  }

  test("should calculate correct result for empty input") {
    testIntersection(List(), List())
  }

  private def testIntersection(inputList: List[Polygon], expectedOutput: List[(Polygon, Polygon)]): Unit = {
    val input: RDD[Polygon] = sc.parallelize(inputList)
    val result: Array[(Polygon, Polygon)] = SelfIntersectionsCalculator.calculate(sc, input).collect()
    result should contain theSameElementsAs expectedOutput
  }

}
