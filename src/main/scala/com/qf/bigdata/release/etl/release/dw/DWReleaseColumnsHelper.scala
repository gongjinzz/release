package com.qf.bigdata.release.etl.release.dw

import scala.collection.mutable.ArrayBuffer

/**
  * DW层投放业务列表
  */
object DWReleaseColumnsHelper {
  /**
    * 曝光
    * 列和目标客户一样，可以在该方法中直接调用selectDWReleaseCustomerColumns()。
    * 但是鉴于selectDWReleaseCustomerColumns()方法有目的性，所以未直接调用。
    * 可以考虑抽出一个具有公共列的方法，供其他方法调用即可，减少代码量
    * @return
    */
  def selectDWReleaseExposureColumns():ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts,'$.idcard') as idcard")
    columns.+=("( cast(date_format(now(),'yyyy') as int) - cast( regexp_extract(get_json_object(exts,'$.idcard'), '(\\\\d{6})(\\\\d{4})(\\\\d{4})', 2) as int) ) as age")
    columns.+=("cast(regexp_extract(get_json_object(exts,'$.idcard'), '(\\\\d{6})(\\\\d{8})(\\\\d{4})', 3) as int) % 2 as gender")
    columns.+=("get_json_object(exts,'$.area_code') as area_code")
    columns.+=("get_json_object(exts,'$.longitude') as longitude")
    columns.+=("get_json_object(exts,'$.latitude') as latitude")
    columns.+=("get_json_object(exts,'$.matter_id') as matter_id")
    columns.+=("get_json_object(exts,'$.model_code') as model_code")
    columns.+=("get_json_object(exts,'$.model_version') as model_version")
    columns.+=("get_json_object(exts,'$.aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns

  }


  /**
    * 目标客户
    *
    * @return
    */
  def selectDWReleaseCustomerColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts,'$.idcard') as idcard")
    columns.+=("( cast(date_format(now(),'yyyy') as int) - cast( regexp_extract(get_json_object(exts,'$.idcard'), '(\\\\d{6})(\\\\d{4})(\\\\d{4})', 2) as int) ) as age")
    columns.+=("cast(regexp_extract(get_json_object(exts,'$.idcard'), '(\\\\d{6})(\\\\d{8})(\\\\d{4})', 3) as int) % 2 as gender")
    columns.+=("get_json_object(exts,'$.area_code') as area_code")
    columns.+=("get_json_object(exts,'$.longitude') as longitude")
    columns.+=("get_json_object(exts,'$.latitude') as latitude")
    columns.+=("get_json_object(exts,'$.matter_id') as matter_id")
    columns.+=("get_json_object(exts,'$.model_code') as model_code")
    columns.+=("get_json_object(exts,'$.model_version') as model_version")
    columns.+=("get_json_object(exts,'$.aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

}
