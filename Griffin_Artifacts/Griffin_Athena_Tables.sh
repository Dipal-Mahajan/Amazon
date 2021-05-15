------Griffin Summary Table-----------

CREATE EXTERNAL TABLE `accuracy_summary_validation`(
  `name` string COMMENT 'from deserializer',
  `tmst` bigint COMMENT 'from deserializer',
  `value` struct<source_wandisco_count:int,target_count:int> COMMENT 'from deserializer',
  `applicationid` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'paths'='applicationId,name,tmst,value')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://gd-ckpetlbatch-prod-hadoop-migrated/griffin/output/total_count'
TBLPROPERTIES (
  'transient_lastDdlTime'='1616666503')


------Griffin Summary View-----------

CREATE OR REPLACE VIEW accuracy_summary_view AS
SELECT
  "name" "Table_Name"
, "value"."source_wandisco_count" "Source_Wandisco_Count"
, "value"."target_count" "Target_Count"
, ("value"."source_wandisco_count" - "value"."target_count") "Row_Count_Difference"
, "round"(((CAST(("value"."source_wandisco_count" - "value"."target_count") AS double) / CAST("value"."source_wandisco_count" AS double)) * 100), 2) "Row_Count_Difference_Percent"
, "from_unixtime"(CAST(("tmst" / 1000) AS bigint)) "Create_ts"
FROM
  accuracy_summary_validation
