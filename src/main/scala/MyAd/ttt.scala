package MyAd

import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer.logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ttt {

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      val sconf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
        .setAppName("dm_release_customer")
        .setMaster("local[4]")

      spark = SparkSession.builder
        .config(sconf)
        .enableHiveSupport()
        .getOrCreate();

      val frame: DataFrame = spark.read.table("hzj_dw_release.dw_release_customer")
      frame.show(10, false)
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
