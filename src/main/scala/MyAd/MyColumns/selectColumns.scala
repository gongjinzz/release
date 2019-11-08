package MyAd.MyColumns

import org.apache.spark.sql.Column

import scala.collection.mutable.ArrayBuffer

object selectColumns {

  def bidding_column: ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts,'$.bidding_status') as bidding_status")
    columns.+=("get_json_object(exts,'$.bidding_type') as bidding_type ")
    columns.+=("get_json_object(exts,'$.bidding_price') as bidding_price")
    columns.+=("get_json_object(exts,'$.aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  def pmp_columns: ArrayBuffer[String] = {
    val column = new ArrayBuffer[String]()
    column.+=("release_session")
    column.+=("release_status")
    column.+=("device_num")
    column.+=("device_type")
    column.+=("sources")
    column.+=("channels")
    column.+=("bidding_type")
    column.+=("bidding_price")
    column.+=("bidding_price as cost_price")
    column.+=("'1'  as bidding_result")
    column.+=("aid")
    column.+=("ct")
    column.+=("bdp_day")
    column
  }

  def rtbColumn: ArrayBuffer[String] = {
    val column = new ArrayBuffer[String]()
    column.+=("release_session")
    column.+=("release_status")
    column.+=("device_num")
    column.+=("device_type")
    column.+=("sources")
    column.+=("channels")
    column.+=("bidding_status")
    column.+=("bidding_type")
    column.+=("bidding_price")
    column.+=("aid")
    column.+=("ct")
    column.+=("bdp_day")

    column
  }

  def rtb_mater: ArrayBuffer[String] = {
    val column = new ArrayBuffer[String]()
    column.+=("release_session")
    column.+=("release_status")
    column.+=("device_num")
    column.+=("device_type")
    column.+=("sources")
    column.+=("channels")
    column.+=("bidding_status")
    column.+=("bidding_type")
    column.+=("bidding_price")
    column.+=("aid")
    column.+=("ct")
    column.+=("bdp_day")
    column
  }

  def rtb_platform: ArrayBuffer[String] = {
    val column = new ArrayBuffer[String]()
    column.+=("release_session")
    column.+=("release_status")
    column.+=("device_num")
    column.+=("device_type")
    column.+=("sources")
    column.+=("channels")
    column.+=("bidding_status")
    column.+=("bidding_type")
    column.+=("bidding_price")
    column.+=("aid")
    column.+=("ct")
    column.+=("bdp_day")
    column
  }

  def rtbMerge: ArrayBuffer[String] = {

    val column = new ArrayBuffer[String]()
    column.+=("m.release_session")
    column.+=("m.release_status")
    column.+=("m.device_num")
    column.+=("m.device_type")
    column.+=("m.sources")
    column.+=("m.channels")
    column.+=("m.bidding_type")
    column.+=("m.bidding_price")
    column.+=("if(isnotnull(p.bidding_price),p.bidding_price, '0') as cost_price")
    column.+=("if(isnotnull(p.bidding_price),'1','0') as bidding_result")
    column.+=("m.aid")
    column.+=("m.ct")
    column.+=("m.bdp_day")
    column
  }

  def dw_bidding: ArrayBuffer[String] = {
    val column = new ArrayBuffer[String]()
    column.+=("release_session")
    column.+=("release_status")
    column.+=("device_num")
    column.+=("device_type")
    column.+=("sources")
    column.+=("channels")
    column.+=("bidding_type")
    column.+=("bidding_price")
    column.+=("cost_price")
    column.+=("bidding_result")
    column.+=("aid")
    column.+=("ct")
    column.+=("bdp_day")
    column
  }

  def tmp_exposure: ArrayBuffer[String] = {
    val column = new ArrayBuffer[String]()
    column.+=("release_session")
    column.+=("release_status")
    column.+=("device_num")
    column.+=("device_type")
    column.+=("sources")
    column.+=("channels")
    column.+=("ct")
    column.+=("bdp_day")
    column
  }

  def dw_ecustomer: ArrayBuffer[String] = {
    val column = new ArrayBuffer[String]()
    column.+=("release_session")
    column.+=("sources")
    column.+=("channels")
    column.+=("device_type")
    column.+=("device_num")
    column.+=("idcard")
    column.+=("gender")
    column.+=("age")
    column.+=("area_code")
    column.+=("longitude")
    column.+=("latitude")
    column.+=("matter_id")
    column.+=("model_code")
    column.+=("model_version")
    column.+=("aid")
    column.+=("bdp_day")
    column
  }

  def dw_exposure: ArrayBuffer[String] = {
    val column = new ArrayBuffer[String]()
    column.+=("ex.release_session as release_session")
    column.+=("ex.release_status as release_status")
    column.+=("ex.device_num as device_num")
    column.+=("ex.device_type as device_type")
    column.+=("ex.sources as sources")
    column.+=("ex.channels as channels")
    column.+=("cu.idcard as idcard")
    column.+=("cu.age as age")
    column.+=("cu.gender as gender")
    column.+=("cu.area_code as area_code")
    column.+=("cu.longitude as longitude")
    column.+=("cu.latitude as latitude")
    column.+=("cu.matter_id as matter_id")
    column.+=("cu.model_code as model_code")
    column.+=("cu.model_version as model_version")
    column.+=("cu.aid as aid")
    column.+=("ex.ct as ct")
    column.+=("ex.bdp_day as bdp_day")
    column
  }

  def selectDMReleaseExposureColumns(): ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("exposure_count")
    columns.+=("exposure_rates")
    columns.+=("bdp_day")
    columns
  }

  def dm_exposureColumn: ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("idcard")
    columns.+=("age")
    columns.+=("getAgeRange('age') as age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  def dm_customerColumn: ArrayBuffer[String] = {

    val column = new ArrayBuffer[String]()
    column.+=("sources")
    column.+=("channels")
    column.+=("device_type")
    column.+=("user_count")
    column.+=("bdp_day")
  }

}
