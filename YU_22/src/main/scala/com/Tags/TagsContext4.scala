package com.Tags
import com.utils.TagUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}


object TagsContext4 {
  def main(args: Array[String]): Unit = {
    if(args.length !=3){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,dirPath,stopwordsPath) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //读取停用单词文件
    val sw: collection.Map[String, Int] = spark.sparkContext.textFile(stopwordsPath).map((_,0)).collectAsMap()

    val stopwords= spark.sparkContext.broadcast(sw)

    //读取字段文件
    val map = spark.sparkContext.textFile(dirPath).map(_.split("\t", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()

    val Appmap: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(map)

    import spark.implicits._
    val logs = spark.read.parquet(inputPath)
    val encoder: Encoder[(List[String], Row)] = Encoders.tuple(
      ExpressionEncoder[List[String]],
      RowEncoder(
        logs.schema
      )
    )
    val baseRdd= logs.filter(TagUtils.oneUserId)
      //接下来所有的标签都在内部实现
      .map(row => {
      //取出用户Id
      val userList = TagUtils.getAllUserId(row)
      (userList, row)
    })(encoder).rdd

    // 构建点集合
    val vertiesRDD= baseRdd.flatMap(tp => {
      val row = tp._2
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
      val AllTag = ADList ++ AppList ++ CNList ++ FA_OSList ++ FA_ISPList ++ FA_NETList ++ KList ++ ZPZCList ++ bussinessList

      val VD: List[(String, Int)] = tp._1.map((_, 0)) ++ AllTag

      tp._1.map(uId => {
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
//    vertiesRDD.take(50).foreach(println)

    // 构建边的集合
    val edges= baseRdd.flatMap(tp => {
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
    })
    edges
//    edges.take(20).foreach(println)

    //构建图
    val graph = Graph(vertiesRDD,edges)

    val vertices = graph.connectedComponents().vertices

    vertices.join(vertiesRDD).map{
      case (uId,(conId,tagsAll)) =>(conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }).take(20).foreach(println)

spark.stop()


  }
}