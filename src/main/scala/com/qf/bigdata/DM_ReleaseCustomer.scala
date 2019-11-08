package com.qf.bigdata

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.etl.release.dm.DMReleaseColumnsHelper
import com.qf.bigdata.release.etl.release.dm.DMReleaseCustomer.{handleReleaseJob, logger}
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
  * 投放目标客户集市
  */
object DM_ReleaseCustomer {

  val logger: Logger = LoggerFactory.getLogger(DM_ReleaseCustomer.getClass)

  /**
    * 目标客户
    * status=01
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String): Unit ={
    val begin=System.currentTimeMillis()
    try{
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //缓存级别
      val saveMode:SaveMode=SaveMode.Overwrite
      val storageLevel:StorageLevel=ReleaseConstant.DEF_STORAGE_LEVEL

      //日志数据
      val customerColumns = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()

      //当天数据
      val customerCondition = $"${ReleaseConstant.DEF_PARTITION}" === lit(bdp_day)

      //读出表的数据 设置查询条件 日志数据 以及缓存级别
      val customerReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_CUSTOMER)
        .where(customerCondition)
        .selectExpr(customerColumns:_*)
        .persist(storageLevel)

      println(s"customerReleaseDF================")
      customerReleaseDF.where("sources='baidu'").show(100,false)

      //渠道统计

    }
  }


  def handleJobs(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
    var spark: SparkSession = null

    try {
      //spark配置参数
      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamoc.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .setAppName("appname")
        .setMaster("local[4]")

      //spark上下文会话
      spark = SparkHelper.createSpark(conf)

      val timeRanges = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)
      for (bdp_day <- timeRanges.reverse) {
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark, appName, bdp_date)
      }
    } catch{
      case ex:Exception=>{
        println(s"DMReleaseCustomer.handleJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(spark!=null){
        spark.stop()
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val appName: String = "dw_shop_log_job"
    val bdp_day_begin:String = "20190613"
    val bdp_day_end:String = "20190613"

    val begin=System.currentTimeMillis()

    val end=System.currentTimeMillis()
    //用的时间
    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }
}