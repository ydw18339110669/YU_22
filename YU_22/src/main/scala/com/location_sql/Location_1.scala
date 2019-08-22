package com.location_sql
/*
地域分布
 */
import org.apache.spark.sql.SparkSession

object Location_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getName)
      .enableHiveSupport().getOrCreate()
    val df = spark.read.parquet("C:\\Users\\Administrator\\Desktop\\资料\\spark文档\\Spark用户画像分析\\res1")
    df.createOrReplaceTempView("DataTmp")

    val res = spark.sql("select\nsum(case when requestmode=1 and processnode>=1 then 1 else 0 end) originalrequests,\nsum(case when requestmode=1 and processnode>=2 then 1 else 0 end) validrequests,\nsum(case when requestmode=1 and processnode=3 then 1 else 0 end) ADrequests,\nsum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) Bidparticipation,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) Successfulbid,\nsum(case when requestmode=2 and iseffective=1 then 1 else 0 end) Showthenumber,\nsum(case when requestmode=3 and iseffective=1 then 1 else 0 end) hits,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) DSPADconsume,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) DSPADcost\nfrom\nDataTmp\ngroup by provincename,cityname")
    res.show()
  }
}
