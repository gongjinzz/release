package MyAd.MyUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Utils {

  def createSpark(name: String): SparkSession = {
    val sconf = new SparkConf()
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.sql.shuffle.partitions", "32")
      .set("hive.merge.mapfiles", "true")
      .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
      .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
      .set("spark.sql.crossJoin.enabled", "true")
      .setAppName(name)
      .setMaster("local[4]")

    val spark = SparkSession
      .builder()
      .config(sconf)
      .enableHiveSupport()
      .getOrCreate()

    return spark
  }
}
