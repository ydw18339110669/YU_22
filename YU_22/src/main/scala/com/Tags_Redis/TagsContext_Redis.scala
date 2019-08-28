package com.Tags_Redis

import com.utils.TagUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object TagsContext_Redis {
  def main(args: Array[String]): Unit = {
    if(args.length !=3){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,dirPath,outputPath) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //读取字典文件
    val tups = spark.sparkContext.textFile(dirPath).map(_.split("\t", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1)))

//    tups.foreachPartition(write2Redis)
    import spark.implicits._
    val logs = spark.read.parquet(inputPath)
    val list= logs.filter(TagUtils.oneUserId)
      //接下来所有的标签都在内部实现
      .rdd.map(row => {
      //取出用户Id
      val userId = TagUtils.getOneUserId(row)
      //接下来通过row打上标签
      val AppList = TagsApp_Redis.makeTags(row)
      AppList
    })
    list.foreach(println)
  }
//def ddd(ss:String):String={
//  "ddd"
//}

  val write2Redis=(it:Iterator[(String,String)]) => {
    val jedis = new Jedis("192.168.153.150", 6379)
    /*
    val pool: JedisPool = new JedisPool("192.168.153.150", 6379)
        val jedis: Jedis = pool.getResource
     */
    it.foreach(tup=>{
      jedis.set(tup._1,tup._2)
    })
  }

}