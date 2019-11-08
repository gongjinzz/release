package MyAd

import MyAd.MyColumns.selectColumns
import MyAd.MyUtils.Utils
import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer.logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object dw_bidding {

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = Utils.createSpark("dw_bidding")

      val columns = selectColumns.bidding_column
      val bdp_day = 20190613
      val dw_bidding_boo = (col("bdp_day") === lit(bdp_day) and col("release_status") === lit("02"))

      //      val bidding: DataFrame = spark.read.table("hzj_ods_release.ods_01_release_session")
      //        .selectExpr(columns: _*)
      //        .where(dw_bidding_boo)
      //        .persist(StorageLevel.MEMORY_AND_DISK)
      //
      //      bidding.write.mode(SaveMode.Overwrite).saveAsTable("hzj_middle.bidding")

      //      bidding.show(10,false)

      //1 as result 怎么做
      //withColumn
      //      val pmpColumn = selectColumns.pmp_columns
      //      val pmpCon = col("bidding_type") === lit("PMP")
      //      val pmp: Dataset[Row] = spark.read.table("hzj_middle.bidding")
      //        .selectExpr(pmpColumn: _*)
      //        .where(pmpCon)
      //        .persist(StorageLevel.MEMORY_AND_DISK)
      //      println("pmp==============================>")
      //      pmp.show(10, false)
      //      pmp.write.mode(SaveMode.Overwrite).saveAsTable("hzj_middle.pmp")

      //
      //      val rtbColumn = selectColumns.rtbColumn
      //      val rtbCon: Column = col("bidding_type") === lit("RTB")
      //      val rtb = spark.read.table("hzj_middle.bidding")
      //        .selectExpr(rtbColumn: _*)
      //        .where(rtbCon)
      //        .persist(StorageLevel.MEMORY_AND_DISK)
      //
      //      rtb.write.mode(SaveMode.Overwrite).saveAsTable("hzj_middle.rtb")


      //      val rtbMasterColumn = selectColumns.rtb_mater
      //      val rtbMasterCon: Column = col("bidding_status") === lit("01")
      //      val rtbMaster = spark.read.table("hzj_middle.rtb")
      //        .selectExpr(rtbMasterColumn: _*)
      //        .where(rtbMasterCon)
      //        .persist(StorageLevel.MEMORY_AND_DISK)
      //
      //      rtbMaster.write.mode(SaveMode.Overwrite).saveAsTable("hzj_middle.rtbMaster")

      val rtbPlatformColumn = selectColumns.rtb_platform
      val rtbPlatformCon: Column = col("bidding_status") === lit("02")
      val rtbPlatform = spark.read.table("hzj_middle.rtb")
        .selectExpr(rtbPlatformColumn: _*)
        .where(rtbPlatformCon)
        .persist(StorageLevel.MEMORY_AND_DISK)

      rtbPlatform.write.mode(SaveMode.Overwrite).saveAsTable("hzj_middle.rtbplatform")
      /*
                        //rtbMaster left join rtbPlatform rtbplatform 为null的说明竞价失败

                        //rtb union pmp 插入表

                        //问题 with怎么实现 1as result 怎么实现 ifnotnull怎么实现
                        //先union在group怎么实现

                        bidding.show(10, false)

                  */
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
