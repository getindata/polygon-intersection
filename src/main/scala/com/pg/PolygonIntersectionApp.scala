package com.pg

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object PolygonIntersectionApp {

  def main(args: Array[String]) {
    val sqlContext = initSqlContext()
    val options = new CliOptions(args.toList)
  }

  private def initSqlContext(): HiveContext = {
    val conf = new SparkConf().setAppName(s"PolygonIntersecion App")
    val sc = new SparkContext(conf)
    new HiveContext(sc)
  }

}