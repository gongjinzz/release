package MyAd

import MyUtils.Utils
import MyColumns.selectColumns
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object dw_register {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = Utils.createSpark("dw_register")

    val bdp_day = 20190613
    val dw_RegisterColumn = selectColumns.dw_registerColumn
    val dw_RegisterCon = (col("bdp_day") === lit(bdp_day) and col("release_status") === lit("06"))
    val dw_register: DataFrame = spark.read.table("hzj_ods_release.ods_01_release_session")
      .where(dw_RegisterCon)
      .selectExpr(dw_RegisterColumn: _*)
    dw_register.show(10, false)
    dw_register.write.mode(SaveMode.Overwrite).insertInto("hzj_dw_release.dw_release_register")

  }
}
