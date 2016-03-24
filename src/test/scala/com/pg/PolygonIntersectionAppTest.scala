package com.pg

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest._

class PolygonIntersectionAppTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  val sc: SparkContext = SparkContextFactory.getSparkContext
  val sqlContext: TestHiveContext = SparkContextFactory.getSqlContext

  test("should calculate intersections and put it in Hive table") {
    val options = new CliOptions(List(
      "--polygons_csv", getClass.getResource("/data.csv").getPath,
      "--result_table", "results"))

    PolygonIntersectionApp.runAnalyze(sc, sqlContext, options)

    val results = sqlContext.sql(s"SELECT * from results").collect()
    results.length shouldEqual 4
  }

}
