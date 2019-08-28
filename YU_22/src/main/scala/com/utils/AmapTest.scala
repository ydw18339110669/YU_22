package com.utils

import com.Tags.BusinessTag
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object AmapTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
//    val list= List("116.310003,39.991957")
//    val rdd = spark.sparkContext.makeRDD(list)
//    val bs = rdd.map(t => {
//      val arr = t.split(",")
//      AmapUtil.getBusinessFromAmap(arr(0).toDouble, arr(1).toDouble)
//    })
//    bs.foreach(println)

    val df: DataFrame = spark.read.parquet("C:\\Users\\Administrator\\Desktop\\资料\\spark文档\\Spark用户画像分析\\res1\\*")
//    import spark.implicits._
    df.rdd.map(row =>{
      val business = BusinessTag.makeTags(row)
      business
    }).foreach(println)

  }
}
