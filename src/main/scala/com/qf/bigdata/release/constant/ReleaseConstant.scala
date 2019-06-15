package com.qf.bigdata.release.constant

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
  * 广告投放常量类
  */
object ReleaseConstant {

  //同步数据(mysql->hive)状态值
  val SYNCDATA_USED :Int = 1
  val SYNCDATA_NOT_USED :Int = 0
  val SYNCDATA_MYSQL_HIVE :String = "release.sync_mysql_hive"

  //range
  val RANGE_DAYS = 2

  //jdbc config
  val JDBC_CONFIG_PATH = "jdbc.properties"
  val JDBC_CONFIG_USER = "user"
  val JDBC_CONFIG_PASS = "password"
  val JDBC_CONFIG_URL = "url"


  //partition
  val DEF_PARTITIONS_FACTOR = 4
  val DEF_FILEPARTITIONS_FACTOR = 10
  val DEF_SOURCE_PARTITIONS = 4
  val DEF_OTHER_PARTITIONS = 8
  val DEF_STORAGE_LEVEL :StorageLevel= StorageLevel.MEMORY_AND_DISK
  val DEF_SAVEMODE:SaveMode  = SaveMode.Overwrite
  val DEF_PARTITION:String = "bdp_day"

  //维度列
  val COL_RELEASE_REQ_ID:String = "release_req_id"
  val COL_RELEASE_SESSION_ID:String = "release_session"
  val COL_RELEASE_SESSION_STATUS:String = "release_status"
  val COL_RELEASE_DEVICE_NUM:String = "device_num"
  val COL_RELEASE_DEVICE_TYPE:String = "device_type"
  val COL_RELEASE_SOURCES:String = "sources"
  val COL_RELEASE_AGE_RANGE:String = "age_range"
  val COL_RELEASE_CHANNELS:String = "channels"
  val COL_RELEASE_GENDER:String = "gender"
  val COL_RELEASE_AREA_CODE:String = "area_code"
  val COL_RELEASE_EXTS:String = "exts"
  val COL_RELEASE_CT:String = "ct"

  //


  //量度列
  val COL_MEASURE_USER_COUNT = "user_count"
  val COL_MEASURE_TOTAL_COUNT = "total_count"



  //ods======================================================
  val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"

  //dw======================================================
  val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"
  val DW_RELEASE_NOTCUSTOMER = "dw_release.dw_release_notcustomer"



  //mid======================================================



  //dm======================================================
  val DM_RELEASE_CUSTOMER_SOURCES = "dm_release.dm_customer_sources"

  val DM_RELEASE_CUSTOMER_CUBE = "dm_release.dm_customer_cube"

  //dm-MYSQL======================================================


}
