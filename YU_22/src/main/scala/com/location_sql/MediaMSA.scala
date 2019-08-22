package com.location_sql
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.matching.Regex

object MediaMSA {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .enableHiveSupport().getOrCreate()
    val df = spark.read.parquet("C:\\Users\\Administrator\\Desktop\\资料\\spark文档\\Spark用户画像分析\\res1")
    df.createOrReplaceTempView("DataTmp")

    import spark.implicits._


    val dsfile: Dataset[String] = spark.read.textFile("C:\\Users\\Administrator\\Desktop\\资料\\spark文档\\Spark用户画像分析\\app_dict.txt")
    val splited= dsfile.map(x => x.split("\t",x.length)).filter(_.length>=5).map(arr =>{
      Demo(arr(4),arr(1))
    })
    splited.createOrReplaceTempView("demo")
//   spark.sql("select * from demo").show()

val res = spark.sql("select\nmax(case when t1.appname = '其他' and t2.name is not null then t2.name else t1.appname end) appname,\nsum(case when requestmode=1 and processnode>=1 then 1 else 0 end) originalrequests,\nsum(case when requestmode=1 and processnode>=2 then 1 else 0 end) validrequests,\nsum(case when requestmode=1 and processnode=3 then 1 else 0 end) ADrequests,\nsum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) Bidparticipation,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) Successfulbid,\nsum(case when requestmode=2 and iseffective=1 then 1 else 0 end) Showthenumber,\nsum(case when requestmode=3 and iseffective=1 then 1 else 0 end) hits,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) DSPADconsume,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) DSPADcost\nfrom\nDataTmp t1\nleft join\ndemo t2\non t1.appid = t2.url\ngroup by t1.appname,t1.appid")
    res.show()


  }

}
case class Demo(url:String,name:String)