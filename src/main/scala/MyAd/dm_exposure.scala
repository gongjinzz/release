package MyAd

import MyAd.MyUtils.Utils
import MyColumns.selectColumns
import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.util.SparkHelper.registerFun
import org.apache.spark.sql.{Column, ColumnName, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object dm_exposure {

  def main(args: Array[String]): Unit = {

    val spark = Utils.createSpark("dm_exposure")
    registerFun(spark)

    val bdp_day = 20190613
    import spark.implicits._

    val Con: Column = $"bdp_day" === lit(bdp_day)
    val dm_customerColumn = selectColumns.dm_customerColumn
    val dm_customer: DataFrame = spark.read.table("hzj_dm_release.dm_release_customer")
      .where(Con)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .selectExpr(dm_customerColumn: _*)

    dm_customer.show(10, false)

    val dw_exporeColumn = selectColumns.dw_ecustomer
    val dw_exposure: DataFrame = spark.read.table("hzj_dw_release.dw_release_exposure")
      .where(Con)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .selectExpr(dw_exporeColumn: _*)

    dw_exposure.show(10, false)

    val dm_exporeColumn = selectColumns.selectDMReleaseExposureColumns()

    val exposureSourceGroupColumns: Seq[Column] = Seq[Column]($"${ReleaseConstant.COL_RELEASE_SOURCES}", $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")


    val a = Seq($"sources", $"channels", $"device_type", $"user_count")


    val dm_exposure: DataFrame = dw_exposure.join(dm_customer, Seq("sources", "channels", "device_type"), "left")
      .groupBy(a: _*)
      .agg(
        countDistinct("device_num").alias("exposure_count"),
        (countDistinct("device_num") / $"user_count").alias("exposure_rates")
      )
      .withColumn("bdp_day", lit(bdp_day))
      .selectExpr(dm_exporeColumn: _*)

    //    dm_exposure.dropDuplicates()

    //    dm_exposure.show(10, false)
    dm_exposure.write.mode(SaveMode.Overwrite).insertInto("hzj_dm_release.dm_release_exposure")

  }
}
