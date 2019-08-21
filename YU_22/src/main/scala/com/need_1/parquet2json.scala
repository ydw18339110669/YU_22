package com.need_1

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object parquet2json {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getName)
      .enableHiveSupport().getOrCreate()
    val df = spark.read.parquet("C:\\Users\\Administrator\\Desktop\\资料\\spark文档\\Spark用户画像分析\\res1")
    df.createOrReplaceTempView("DataTmp")

    val res = spark.sql("select provincename,cityname,count(*) ct from DataTmp group by provincename,cityname")

//    val prop = new Properties()
//    prop.setProperty("user","root")
//    prop.setProperty("password","123456")
//    val url ="jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8"
//    res.write.jdbc(url,"DataTmp",prop)

    res.write.partitionBy("provincename","cityname").json("C:\\\\Users\\\\Administrator\\\\Desktop\\\\资料\\\\spark文档\\\\Spark用户画像分析\\\\res2")



  }
}
