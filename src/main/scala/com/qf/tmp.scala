package com.qf

import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf

import scala.collection.mutable


object tmp {

  //spark配置参数
  val sconf = new SparkConf()
    .set("hive.exec.dynamic.partition", "true")
    .set("hive.exec.dynamic.partition.mode", "nonstrict")
    .set("spark.sql.shuffle.partitions", "32")
    .set("hive.merge.mapfiles", "true")
    .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
    .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
    .set("spark.sql.crossJoin.enabled", "true")
    //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
    .setAppName("tmp")
    .setMaster("local[4]")

  val spark = SparkHelper.createSpark(sconf)

//  val colNames:mutable.Seq[String]

//  val tableDF = spark.read.table("hzj_ods_release.ods_01_release_session")
//    .selectExpr(colNames: _*)
//
//  tableDF.show(10)
}
