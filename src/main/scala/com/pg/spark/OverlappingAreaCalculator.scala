package com.pg.spark

import model.{OverlappingPair, Polygon}
import org.apache.spark.rdd.RDD

object OverlappingAreaCalculator {

  /**
    * For every pair of polygons (a, b) calculate how much area of b (in percents) is inside a
    */
  def calculate(pairs: RDD[(Polygon, Polygon)]): RDD[OverlappingPair] = {
    pairs.map {
      case (a, b) =>
        val bArea = b.geometry.getArea
        val abIntersectionArea = a.geometry.intersection(b.geometry).getArea
        if (bArea.abs < 1e-9)
          // bArea is zero
          OverlappingPair(a.id, b.id, 1.0)
        else
          OverlappingPair(a.id, b.id, abIntersectionArea / bArea)

    }
  }
}
