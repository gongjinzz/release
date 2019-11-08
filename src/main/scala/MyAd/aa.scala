package MyAd

import MyAd.MyColumns.selectColumns
import MyAd.MyUtils.Utils
import com.qf.bigdata.release.constant.ReleaseConstant
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel

object aa {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = Utils.createSpark("aa")
    val columns = selectColumns.bidding_column
    val bdp_day = 20190613
    val dw_bidding_boo = (col("bdp_day") === lit(bdp_day) and col("release_status") === lit("02"))

    val bidding: DataFrame = spark.read.table("hzj_ods_release.ods_01_release_session")
      .where(dw_bidding_boo)
      .selectExpr(columns: _*)
      .persist(StorageLevel.MEMORY_AND_DISK)

    bidding.show(10, false)

    val pmpColumn = selectColumns.pmp_columns
    val pmpCon = col("bidding_type") === lit("PMP")
    val pmp: Dataset[Row] = bidding
      .where(pmpCon)
      .selectExpr(pmpColumn: _*)
      .persist(StorageLevel.MEMORY_AND_DISK)
    println("pmp==============================>")
    pmp.show(10, false)

    val rtbColumn = selectColumns.rtbColumn
    val rtbCon: Column = col("bidding_type") === lit("RTB")
    val rtb = bidding
      .where(rtbCon)
      .selectExpr(rtbColumn: _*)
      .persist(StorageLevel.MEMORY_AND_DISK)
    rtb.show(10, false)

    val rtbMasterColumn = selectColumns.rtb_mater
    val rtbMasterCon: Column = col("bidding_status") === lit("01")

    val rtbMaster: Dataset[Row] = rtb.where(rtbMasterCon)
      .selectExpr(rtbMasterColumn: _*)
      .persist(StorageLevel.MEMORY_AND_DISK)
    rtbMaster.show(10, false)

    val rtbPlatformColumn = selectColumns.rtb_platform
    val rtbPlatformCon = col("bidding_status") === lit("02")
    val rtbPlatform: Dataset[Row] = rtb.where(rtbPlatformCon)
      .selectExpr(rtbPlatformColumn: _*)
      .persist(StorageLevel.MEMORY_AND_DISK)
    rtbPlatform.show(10, false)


    val rtbMergeColumn = selectColumns.rtbMerge
    val joinColumn = Seq("release_session")
    //    join的字段
    val rtbMerge: DataFrame = rtbMaster.alias("m")
      .join(rtbPlatform.alias("p"), joinColumn, "left")
      .selectExpr(rtbMergeColumn: _*)
    rtbMerge.show(10, false)

    val dw_biddingColumn = selectColumns.dw_bidding
    val dw_bidding: DataFrame = pmp.union(rtbMerge).selectExpr(dw_biddingColumn: _*)
    dw_bidding.show(10, false)

    dw_bidding.write.mode(SaveMode.Overwrite).insertInto("hzj_dw_release.dw_release_bidding")

    //    import spark.implicits._
    //    col("") === lit("")
    //    col("") === $""
    //    val tmp: DataFrame = rtbMaster.alias("m")
    //      .join(rtbPlatform.alias("p"), $"m.release_session" === $"p.release_session" and $"m.sources" === $"p.sources", "left")
    //      .selectExpr(rtbMergeColumn: _*)
    //    tmp.show(10, false)

  }
}
