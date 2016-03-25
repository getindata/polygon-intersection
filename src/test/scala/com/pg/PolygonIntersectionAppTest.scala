package com.pg

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest._

class PolygonIntersectionAppTest extends FunSuite with Matchers {

  val sc: SparkContext = SparkContextFactory.getSparkContext
  val sqlContext: TestHiveContext = SparkContextFactory.getSqlContext

  test("should calculate intersections and put it in Hive table") {
    val options = new CliOptions(List(
      "--polygons_csv", getClass.getResource("/data.csv").getPath,
      "--result_table", "results"))

    PolygonIntersectionApp.runCalculations(sc, sqlContext, options)

    val results: Array[Row] = sqlContext.sql(s"SELECT aId, bId, bFraction from results").collect()
    val expectations = Map(
      (0L, 0L) -> 1.0,
      (0L, 1L) -> 1.0,
      (1L, 1L) -> 1.0,
      (1L, 0L) -> 0.25,
      (2L, 2L) -> 1.0
    )

    results.length shouldEqual 5
    for (r <- results) {
      val idPair = (r.getLong(0), r.getLong(1))
      val bFraction = r.getDouble(2)
      expectations(idPair) shouldEqual bFraction +- 0.01
    }
  }

}
