package com.pg.spark

import model.Polygon
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spatialspark.join.BroadcastSpatialJoin
import spatialspark.operator.SpatialOperator.Intersects

object SelfIntersectionsCalculator {

  /**
    * Calculate pairs of Polygons which intersects with each other
    */
  def calculate(sc: SparkContext, polygons: RDD[Polygon]): RDD[(Polygon, Polygon)] = {
    val geometryById = polygons.map(p => (p.id, p.geometry))
    geometryById.cache()

    val intersectedIds: RDD[(Long, Long)] = BroadcastSpatialJoin(sc, geometryById, geometryById, Intersects, 0.0)

    // join with source data
    val polgonsById = polygons.map(p => (p.id, p))
    polgonsById.cache()
    intersectedIds.join(polgonsById).map(_._2).join(polgonsById).map(_._2)
  }

}
