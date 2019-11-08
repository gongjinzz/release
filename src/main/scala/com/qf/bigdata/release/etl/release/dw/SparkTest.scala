package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer.{handleJobs, handleReleaseJob, logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, RowFactory, SparkSession}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._

object SparkTest {


  /**
    * 投放目标客户
    * @param appName
    */
  def handleJobs(appName :String) :Unit = {
    var spark :SparkSession = null
    try{
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
        .setAppName(appName)
        .setMaster("local[4]")



      //spark上下文会话
       spark = SparkSession.builder
        .config(sconf)
        .enableHiveSupport()
        .getOrCreate();

      //加载自定义函数
      //spark.udf.register("getTimeSegment", QFUdf.getTimeSegment _)
      val df = spark.read.csv("")


      val relCols = new ArrayBuffer[String]()
      relCols.+=("release_session")
      relCols.+=("release_status")

      import org.apache.spark.sql.functions._




    }catch{
      case ex:Exception => {
        //println(s"DWReleaseCustomer.handleJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }



  def main(args: Array[String]): Unit = {

    //动态参数接收
    //val Array(appName, bdp_day_begin, bdp_day_end) = args


    val appName: String = "dw_release_customer_job"
    val bdp_day_begin:String = "20190613"
    val bdp_day_end:String = "20190613"

    val begin = System.currentTimeMillis()
    handleJobs(appName)
    val end = System.currentTimeMillis()



  }

}
