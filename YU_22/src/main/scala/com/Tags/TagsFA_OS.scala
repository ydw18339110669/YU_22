package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagsFA_OS extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数

    /*
 uuid: String,	设备唯一标识
    client: Int,	设备类型 （1：android 2：ios 3：wp）
     */
    val row = args(0).asInstanceOf[Row]
    val client = row.getAs[Int]("client")
      if(client==1){
        list :+= (" D00010001",1)
      }else if(client==2){
        list :+= (" D00010002",1)
      }else if(client==3){
        list :+= (" D00010003",1)
      }else{
        list :+= (" D00010004",1)
      }
    list

  }
}
