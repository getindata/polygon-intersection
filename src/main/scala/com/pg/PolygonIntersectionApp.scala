package com.pg

import com.pg.spark.{OverlappingAreaCalculator, SelfIntersectionsCalculator}
import com.vividsolutions.jts.io.WKTReader
import model.{OverlappingPolygons, Polygon}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object PolygonIntersectionApp {

  def main(args: Array[String]) {
    val sc = new SparkContext(
      new SparkConf().setAppName(s"PolygonIntersection App"))
    val sqlContext = new HiveContext(sc)
    val options = new CliOptions(args.toList)

    runCalculations(sc, sqlContext, options)
  }

  def runCalculations(sc: SparkContext, sqlContext: HiveContext, options: CliOptions): Unit = {
    // read CSV to DF
    val polygonsDataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .load(options.polygonsCsv)

    // create Polygons
    val polygons: RDD[Polygon] = polygonsDataFrame.rdd.zipWithIndex().map {
      case (row, id) => Polygon(id, new WKTReader().read(row.getString(0)))
    }
    polygons.cache()

    // calculate intersection
    val intersectedPolygons: RDD[(Polygon, Polygon)] = SelfIntersectionsCalculator.calculate(sc, polygons)

    // for every row (a, b) calculate how much of b intersects a
    val result: RDD[OverlappingPolygons] = OverlappingAreaCalculator.calculate(intersectedPolygons)

    // save result in Hive
    import sqlContext.implicits._
    result.toDF.write.saveAsTable(options.resultTable)
  }

}