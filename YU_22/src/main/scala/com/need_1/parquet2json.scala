package com.need_1

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

object parquet2json {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getName)
      .enableHiveSupport().getOrCreate()
    val df = spark.read.parquet("C:\\Users\\Administrator\\Desktop\\资料\\spark文档\\Spark用户画像分析\\res1")
    df.createOrReplaceTempView("DataTmp")

    val res = spark.sql("select provincename,cityname,count(*) ct from DataTmp group by provincename,cityname")

    //写入数据库
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    res.write.jdbc(load.getString("jdbc.url"),load.getString("jdbc.tablename"),prop)

    //以属性分区
//    res.write.partitionBy("provincename","cityname").json("C:\\\\Users\\\\Administrator\\\\Desktop\\\\资料\\\\spark文档\\\\Spark用户画像分析\\\\res2")
//指定一个分区存储
//    res.coalesce(1).write.json("C:\\Users\\Administrator\\Desktop\\资料\\spark文档\\Spark用户画像分析\\res3")


  }
}
