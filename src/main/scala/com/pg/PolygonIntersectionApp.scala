package com.pg

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object PolygonIntersectionApp {

  def main(args: Array[String]) {
    val sqlContext = initSqlContext()
    val options = new CliOptions(args.toList)
    runAnalyze(sqlContext, options)
  }

  def runAnalyze(sqlContext: HiveContext, options: CliOptions): Unit = {
    // wczytaj csv to DF
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(options.polygonsCsv)

    // nadaj ID'ki

    // policz przeciecie

    // dla kazdego wiersza a, b policz ile procent b zachodzi na a

    // zapisz wynik w tabel Hive
    df.write.saveAsTable(options.resultTable)
  }

  private def initSqlContext(): HiveContext = {
    val conf = new SparkConf().setAppName(s"PolygonIntersecion App")
    val sc = new SparkContext(conf)
    new HiveContext(sc)
  }

}