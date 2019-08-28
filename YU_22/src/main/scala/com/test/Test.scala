package com.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    if(args.length !=2){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val logs: RDD[String] = spark.sparkContext.textFile(inputPath)


//    val backlog: RDD[String] = logs.map(line => {
//      MyJsonUtil.getBusinessFromAmap(line)
//    })
//    val tups: RDD[(String, Int)] = backlog.flatMap(_.split(",").map((_,1)))
//    val res: RDD[(String, Int)] = tups.reduceByKey(_+_)
//    res.foreach(println)




    logs.map(row =>{
       MyJsonUtil.getBusinessFromAmap(row)
    }).flatMap(line => {
      line.split(",").map((_,1))
    }).reduceByKey(_+_).foreach(println)

    logs.map(row =>{
      MyJsonUtil2.getBusinessFromAmap(row)
    }).flatMap(line =>{
      line.split(";").map((_,1))
    })reduceByKey(_+_)foreach(println)



  }
}
