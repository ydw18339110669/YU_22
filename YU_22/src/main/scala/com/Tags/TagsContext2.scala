package com.Tags

import com.utils.TagUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

object TagsContext2 {
  def main(args: Array[String]): Unit = {
    if(args.length !=4){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,dirPath,stopwordsPath,outputPath) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //读取停用单词文件
    val sw: collection.Map[String, Int] = spark.sparkContext.textFile(stopwordsPath).map((_,0)).collectAsMap()

    val stopwords= spark.sparkContext.broadcast(sw)

    //读取字段文件
    val map = spark.sparkContext.textFile(dirPath).map(_.split("\t", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()

    val Appmap: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(map)

    //    val sqlContext = new SQLContext(sc)
    //    val df = sqlContext.read.parquet(inputPath)
    import spark.implicits._
    val logs = spark.read.parquet(inputPath)
    val list: Dataset[(String, List[(String, Int)])] = logs.filter(TagUtils.oneUserId)
      //接下来所有的标签都在内部实现
      .map(row => {
      //取出用户Id
      val userId = TagUtils.getOneUserId(row)
      //接下来通过row打上标签
      val ADList = TagsAD.makeTags(row)
      val AppList = TagsApp.makeTags(row, Appmap)
      val CNList = TagsCN.makeTags(row)
      val FA_OSList = TagsFA_OS.makeTags(row)
      val FA_ISPList = TagsFA_ISP.makeTags(row)
      val FA_NETList = TagsFA_NET.makeTags(row)
      val KList = TagsK.makeTags(row, stopwords)
      val ZPZCList = TagsZPZC.makeTags(row)
      val bussinessList = BusinessTag.makeTags(row)
//      (userId, ADList ++ AppList ++ CNList ++ FA_OSList ++ FA_ISPList ++ FA_NETList ++ KList ++ ZPZCList ++ bussinessList)
      (userId,  bussinessList)
    })

    list.rdd.reduceByKey((list1,list2)=>{
//      (list1 ::: list2)
//        .groupBy(_._1)
//        .map(tup=>{
//          val v = tup._2.foldLeft(0)(_+_._2)
//          (tup._1,v)
//        }).toList
      (list1 ::: list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList


    }).foreach(println)

  }
}