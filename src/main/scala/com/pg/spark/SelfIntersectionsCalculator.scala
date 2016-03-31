package com.pg.spark

import model.Polygon
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spatialspark.join.PartitionedSpatialJoin
import spatialspark.operator.SpatialOperator.Intersects
import spatialspark.partition.fgp.FixedGridPartitionConf
import spatialspark.util.MBR

object SelfIntersectionsCalculator {

  /**
    * Calculate pairs of Polygons which intersects with each other
    */
  def calculate(sc: SparkContext, polygons: RDD[Polygon]): RDD[(Polygon, Polygon)] = {
    val geometryById = polygons.filter(!_.geometry.isEmpty).map(p => (p.id, p.geometry))
    if (geometryById.count() == 0) {
      return sc.emptyRDD
    }
    geometryById.cache()

    val extent = polygons.map(_.geometry.getEnvelopeInternal)
      .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
      .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))

    val conf = new FixedGridPartitionConf(32, 32, new MBR(extent._1, extent._2, extent._3, extent._4))

    val intersectedIds: RDD[(Long, Long)] =
      PartitionedSpatialJoin(sc, geometryById, geometryById, Intersects, 0.0, conf)

    // join with source data
    val polygonsById = polygons.map(p => (p.id, p))
    polygonsById.cache()
    intersectedIds.join(polygonsById).map(_._2).join(polygonsById).map(_._2)
  }

}
