package MyAd

import MyUtils.Utils
import MyColumns.selectColumns
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object dw_exposure {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = Utils.createSpark("dw_exposure")

    val bdp_day = 20190613
    val release_status = "03"
    import spark.implicits._
    val tmpCon: Column = $"bdp_day" === lit(bdp_day) and $"release_status" === lit(release_status)
    val tmpColumns = selectColumns.tmp_exposure

    val tmpExpore: DataFrame = spark.read.table("hzj_ods_release.ods_01_release_session")
      .where(tmpCon)
      .selectExpr(tmpColumns: _*)

    tmpExpore.show(10, false)
    val dwCustomerColumn = selectColumns

    val dw_customerCon: Column = $"bdp_day" === lit(bdp_day)
    val dw_costomerColumn = selectColumns.dw_ecustomer
    val dw_customer: DataFrame = spark.read.table("hzj_dw_release.dw_release_customer")
      .where(dw_customerCon)
      .selectExpr(dw_costomerColumn: _*)

    dw_customer.show(10, false)


    val dw_exposureColumn = selectColumns.dw_exposure
    val dw_exposure: DataFrame = tmpExpore.alias("ex")
      .join(dw_customer.alias("cu"), $"ex.release_session" === $"cu.release_session", "left")
      .selectExpr(dw_exposureColumn: _*)
    dw_exposure.show(10, false)

    dw_exposure.write.mode(SaveMode.Overwrite).insertInto("hzj_dw_release.dw_release_exposure")
  }
}
