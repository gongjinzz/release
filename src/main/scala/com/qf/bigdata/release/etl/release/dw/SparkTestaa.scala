package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer.logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object SparkTestaa {

  def main(args: Array[String]): Unit = {


    var spark: SparkSession = null
    try {
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
        .setAppName("SparkTestaa")
        .setMaster("local[4]")

      //spark上下文会话
      spark = SparkSession.builder
        .config(sconf)
        .enableHiveSupport()
        .getOrCreate();

      val columns = new ArrayBuffer[String]()
      columns.+=("release_session")
      columns.+=("release_status")
      columns.+=("device_num")
      columns.+=("device_type")
      columns.+=("sources")
      columns.+=("channels")
      columns.+=("get_json_object(exts,'$.idcard') as idcard")
      columns.+=("( cast(date_format(now(),'yyyy') as int) - cast( regexp_extract(get_json_object(exts,'$.idcard'), '(\\\\d{6})(\\\\d{4})(\\\\d{4})', 2) as int) ) as age")
      columns.+=("cast(regexp_extract(get_json_object(exts,'$.idcard'), '(\\\\d{6})(\\\\d{8})(\\\\d{4})', 3) as int) % 2 as gender")
      columns.+=("get_json_object(exts,'$.area_code') as area_code")
      columns.+=("get_json_object(exts,'$.longitude') as longitude")
      columns.+=("get_json_object(exts,'$.latitude') as latitude")
      columns.+=("get_json_object(exts,'$.matter_id') as matter_id")
      columns.+=("get_json_object(exts,'$.model_code') as model_code")
      columns.+=("get_json_object(exts,'$.model_version') as model_version")
      columns.+=("get_json_object(exts,'$.aid') as aid")
      columns.+=("ct")
      columns.+=("bdp_day")

      val bdp_day = 20190613
      val column: Column = col("bdp_day") === lit(bdp_day) and col("release_status") === lit("01")

      val frame: DataFrame = spark.read.table("hzj_ods_release.ods_01_release_session")
        .where(column)
        .selectExpr(columns: _*)
        .repartition(4)

      println(s"customerReleaseDF================")
      frame.show(10, false)
      frame.write.mode(SaveMode.Overwrite).insertInto("hzj_dw_release.dw_release_customer")

    } catch {
      case ex: Exception => {
        //println(s"DWReleaseCustomer.handleJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
