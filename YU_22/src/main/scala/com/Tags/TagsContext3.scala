package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}


object TagsContext3 {
  def main(args: Array[String]): Unit = {
    if(args.length !=5){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,dirPath,stopwordsPath,outputPath,days) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //读取停用单词文件
    val sw: collection.Map[String, Int] = spark.sparkContext.textFile(stopwordsPath).map((_,0)).collectAsMap()

    val stopwords= spark.sparkContext.broadcast(sw)

    //读取字段文件
    val map = spark.sparkContext.textFile(dirPath).map(_.split("\t", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()

    val Appmap: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(map)

      //todo 调用HbaseAPI
    //加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName =load.getString("hbase.TableName")
    val host = load.getString("hbase.host")

    //创建Hadoop任务
    val configuration = spark.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",host)

    //创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin

    //判断表是否可用
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      //创建表操作
      val hbaseTable = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      hbaseTable.addFamily(descriptor)
      hbadmin.createTable(hbaseTable)
      hbadmin.close()
      hbconn.close()
    }

    //创建JobConf
    val jobconf = new JobConf(configuration)
    //指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)



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
      (userId, ADList ++ AppList ++ CNList ++ FA_OSList ++ FA_ISPList ++ FA_NETList ++ KList ++ ZPZCList ++ bussinessList)
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

    }).filter(_._1!=null).map{
      case(userid,userTag)=>{
          val put = new Put(Bytes.toBytes(userid))
          val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
          put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
          (new ImmutableBytesWritable(),put)

      }
    }.saveAsHadoopDataset(jobconf)

  }
}