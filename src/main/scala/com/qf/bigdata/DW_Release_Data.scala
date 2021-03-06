package com.qf.bigdata

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.enums.ReleaseStatusEnum
import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer.{handleJobs, handleReleaseJob, logger}
import com.qf.bigdata.release.etl.release.dw.{DWReleaseColumnsHelper, DWReleaseCustomer}
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * 目标客户
  * status=01
  */
object DW_Release_Data {

  val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)

  def handleReleaseJob(spark: SparkSession, appName: String, bdp_day: String): Unit = {
    val begin = System.currentTimeMillis()
    try {
      import spark.implicits._
      import org.apache.spark.sql.functions._

      //缓存级别
      val storageLevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode: SaveMode = SaveMode.Overwrite

      //日志数据 选择列
      val customerColumns= DWReleaseColumnsHelper.selectDWReleaseCustomerColumns()

      //当天的数据
      val customerReleaseCondition = ( col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day) and col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") === lit(ReleaseStatusEnum.CUSTOMER.getCode))

      val customerReleaseDF: DataFrame = SparkHelper.readTableData(spark,ReleaseConstant.DEF_PARTITION,customerColumns)
        .where(customerReleaseCondition)
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITIONS)

      println(s"customerReleaseDF================")
      customerReleaseDF.show(10,false)

      //目标客户
      //SparkHelper.writeTableData(customerReleaseDF, ReleaseConstant.DW_RELEASE_CUSTOMER, saveMode)
    }catch{
      case ex:Exception => {
        println(s"DWReleaseCustomer.handleReleaseJob occur exception：app=[$appName],date=[${bdp_day}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      println(s"DWReleaseCustomer.handleReleaseJob End：appName=[${appName}], bdp_day=[${bdp_day}], use=[${System.currentTimeMillis() - begin}]")
    }
  }

  /**
    * 投放目标客户
    */

  def handleJobs(appName:String,bdp_day_begin:String, bdp_day_end:String): Unit ={
    var spark :SparkSession = null
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
        .setAppName(appName)
        .setMaster("local[4]")

      spark=SparkHelper.createSpark(sconf)

      val timeRanges = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
      for(bdp_day <- timeRanges.reverse){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark, appName, bdp_date)
      }
    }catch{
      case ex:Exception => {
        println(s"DWReleaseCustomer.handleJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }

  def main(args: Array[String]): Unit = {

//    System.setProperty("hadoop.home.dir", "D:\\framework\\hadoop-win\\hadoop-common-2.2.0-bin-master")
    val appName: String = "dw_release_customer_job"
    val bdp_day_begin:String = "20190613"
    val bdp_day_end:String = "20190613"

    val begin = System.currentTimeMillis()
    handleJobs(appName, bdp_day_begin, bdp_day_end)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }
}
