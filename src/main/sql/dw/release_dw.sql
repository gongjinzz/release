--1 创建主题数据层数据库
create database dw_release;


--2 目标客户主题
create external table if not exists dw_release.dw_release_customer(
  release_req_id string comment '投放请求id',
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明',
  device_num string comment '设备唯一编码',
  device_type string comment '1 android| 2 ios | 9 其他',
  sources string comment '渠道',
  channels string comment '通道',
  exts string comment '扩展信息(参见下面扩展信息描述)',
  ct bigint comment '创建时间'
) partitioned by (bdp_day string)
stored as parquet
location '/data/release/ods/release_session/'