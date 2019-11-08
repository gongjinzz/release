package MyAd

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer.logger
import com.qf.bigdata.release.util.SparkHelper.registerFun
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


object dm_release_customer {

  def main(args: Array[String]): Unit = {

    var spark: SparkSession = null
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
        .setAppName("dm_release_customer")
        .setMaster("local[4]")

      //spark上下文会话
      spark = SparkSession.builder
        .config(sconf)
        .enableHiveSupport()
        .getOrCreate()

      registerFun(spark)

      val dw_release_customer = new ArrayBuffer[String]()
      dw_release_customer.+=("release_session")
      dw_release_customer.+=("release_status")
      dw_release_customer.+=("device_num")
      dw_release_customer.+=("device_type")
      dw_release_customer.+=("sources")
      dw_release_customer.+=("channels")
      dw_release_customer.+=("idcard")
      dw_release_customer.+=("age")
      dw_release_customer.+=("getAgeRange('age') as age_range")
      dw_release_customer.+=("gender")
      dw_release_customer.+=("area_code")
      dw_release_customer.+=("ct")

      val bdp_day = 20190613
      val timeB: Column = col("bdp_day") === lit(s"${bdp_day}")
      val dw_customer: DataFrame = spark.read
        .table("hzj_dw_release.dw_release_customer")
        .selectExpr(dw_release_customer: _*)
        .where(timeB)
        .persist(StorageLevel.MEMORY_AND_DISK)

      println(s"dw_release_customer============================>")
      dw_customer.show(10, false)

      val dm_customer = new ArrayBuffer[String]()
      dm_customer.+=("sources")
      dm_customer.+=("channels")
      dm_customer.+=("device_type")
      dm_customer.+=("user_count")
      dm_customer.+=("total_count")
      dm_customer.+=("bdp_day")

      //            val groupc = new ArrayBuffer[String]()
      //            groupc.+=("sources")
      //            groupc.+=("channels")
      //            groupc.+=("device_type")
      val _groupby: Seq[ColumnName] = Seq[ColumnName](new ColumnName("sources"),
        new ColumnName("channels"),
        new ColumnName("device_type"))

      //      val dm_frame: DataFrame = dw_customer.groupBy(_groupby: _*)
      //        .agg(
      //          countDistinct("device_num").alias("user_count"),
      //          count("release_session").alias("total_count")
      //        )
      //        .withColumn("bdp_day", lit(bdp_day))
      //        .selectExpr(dm_customer: _*)


      //      dm_frame.write.mode(SaveMode.Overwrite).insertInto("hzj_dm_release.dm_release_customer")
      //      println(s"end write")

      //with cube
      val cubeArray = new ArrayBuffer[String]()
      cubeArray.+=("sources")
      cubeArray.+=("channels")
      cubeArray.+=("device_type")
      cubeArray.+=("gender")
      cubeArray.+=("area_code")
      cubeArray.+=("age_range")
      cubeArray.+=("user_count")
      cubeArray.+=("total_count")
      cubeArray.+=("bdp_day")

      val cubeGroup = Seq[Column](
        new ColumnName("sources"),
        new ColumnName("channels"),
        new ColumnName("device_type"),
        new ColumnName("age_range"),
        new ColumnName("gender"),
        new ColumnName("area_code")
      )

      //      val cube_customer: DataFrame = dw_customer.groupBy(cubeGroup: _*)
      //        .agg(
      //          countDistinct("device_num").alias("user_count"),
      //          count("release_session").alias("total_count")
      //        )
      //        .withColumn("bdp_day", lit(bdp_day))
      //        .selectExpr(cubeArray: _*)
      //      cube_customer.write.mode(SaveMode.Overwrite).insertInto("hzj_dm_release.dm_customer_cube")
      //      println("end write===============>")
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
