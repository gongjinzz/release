package MyAd

import MyUtils.Utils
import MyColumns.selectColumns
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object dm_register {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = Utils.createSpark("dm_register")
    /*
    group by
device_type,
sources,
channels
     */
    val bdp_day = 20190613
    val dm_registerCon = col("bdp_day") === lit(bdp_day)
    val dw_registerColumn = selectColumns.dm_registerColumn
    import spark.implicits._
    val dm_registerGroup = Seq($"sources", $"channels", $"device_type")


    val dm_register: DataFrame = spark.read.table("hzj_dw_release.dw_release_register")
      .where(dm_registerCon)
      .groupBy(dm_registerGroup: _*)
      .agg(
        count("sources").alias("cnt")
      )
      .withColumn("bdp_day",lit(bdp_day))
      .selectExpr(dw_registerColumn: _*)
    dm_register.show(10, false)


    dm_register.write.mode(SaveMode.Overwrite).insertInto("hzj_dm_release.dm_release_register")
  }
}
