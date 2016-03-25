package com.pg.spark

import model.{OverlappingPolygons, Polygon}
import org.apache.spark.rdd.RDD

object OverlappingAreaCalculator {

  private val EPS: Double = 1e-9

  /**
    * For every pair of polygons (a, b) calculate how much area of b (in percents) is inside a
    */
  def calculate(pairs: RDD[(Polygon, Polygon)]): RDD[OverlappingPolygons] = {
    pairs.flatMap {
      case (a, b) =>
        val bArea = b.geometry.getArea
        if (bArea.abs < EPS) {
          None
        } else {
          val abIntersectionArea = a.geometry.intersection(b.geometry).getArea
          Some(OverlappingPolygons(a.id, b.id, abIntersectionArea / bArea))
        }
    }
  }
}
