package com.qf.bigdata.release.etl.release.dw

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object SparkDmTest {

  def main(args: Array[String]): Unit = {

    val sconf = new SparkConf()
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.sql.shuffle.partitions", "32")
      .set("hive.merge.mapfiles", "true")
      .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
      .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
      .set("spark.sql.crossJoin.enabled", "true")
      .setAppName("SparkDmTest")
      .setMaster("local[4]")

    val spark: SparkSession = SparkSession
      .builder()
      .config(sconf)
      .getOrCreate()

    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")

  }
}
