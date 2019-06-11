package com.qf.bigdata.release.etl.release.dm

import scala.collection.mutable.ArrayBuffer

object DMReleaseColumnsHelper {

  /**
    * 目标客户
    * @return
    */
  def selectDMReleaseCustomerColumns():ArrayBuffer[String] ={
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
