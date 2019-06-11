package com.qf.bigdata.release.etl.release.dw

import scala.collection.mutable.ArrayBuffer

/**
  * DW层投放业务列表
  */
object DWReleaseColumnsHelper {


  /**
    * 目标客户
    * @return
    */
  def selectDWReleaseCustomerColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_req_id")
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("exts")
    columns.+=("bdp_day")
    columns
  }

}
