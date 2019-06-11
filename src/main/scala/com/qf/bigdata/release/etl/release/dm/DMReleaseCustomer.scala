package com.qf.bigdata.release.etl.release.dm

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.enums.ReleaseStatusEnum
import com.qf.bigdata.release.etl.release.dw.{DWReleaseColumnsHelper}
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class DMReleaseCustomer{

}


/**
  * 投放目标客户数据集市
  */
object DMReleaseCustomer {

  val logger :Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)

  /**
    * 目标客户
    * status=01
    */
  def handleReleaseJob(spark:SparkSession, appName :String, bdp_day:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //缓存级别
      val saveMode:SaveMode = SaveMode.Overwrite

      //日志数据
      val customerColumns = DMReleaseColumnsHelper.selectDMReleaseCustomerColumns()

      //当天数据
      val customerReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day))
      val customerReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_CUSTOMER, customerColumns)
        .where(customerReleaseCondition)
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITIONS)
      println(s"customerReleaseDF================")
      customerReleaseDF.show(10,false)

      //目标客户
      SparkHelper.writeTableData(customerReleaseDF, ReleaseConstant.DM_RELEASE_CUSTOMER, saveMode)

    }catch{
      case ex:Exception => {
        println(s"DMReleaseCustomer.handleReleaseJob occur exception：app=[$appName],date=[${bdp_day}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      println(s"DMReleaseCustomer.handleReleaseJob End：appName=[${appName}], bdp_day=[${bdp_day}], use=[${System.currentTimeMillis() - begin}]")
    }
  }



  /**
    * 投放目标客户
    * @param appName
    */
  def handleJobs(appName :String, bdp_day_begin:String, bdp_day_end:String) :Unit = {
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
        .setAppName(appName)
      //.setMaster("local[4]")

      //spark上下文会话
      spark = SparkHelper.createSpark(sconf)

      val timeRanges = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
      for(bdp_day <- timeRanges.reverse){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark, appName, bdp_date)
      }

    }catch{
      case ex:Exception => {
        println(s"DMReleaseCustomer.handleJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }


  def main(args: Array[String]): Unit = {

    val Array(appName, bdp_day_begin, bdp_day_end) = args

    //    val appName: String = "dw_shop_log_job"
    //    val bdp_day_begin:String = "20190525"
    //    val bdp_day_end:String = "20190525"

    val begin = System.currentTimeMillis()
    handleJobs(appName, bdp_day_begin, bdp_day_end)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }


}
