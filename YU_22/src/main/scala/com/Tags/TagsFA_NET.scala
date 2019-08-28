package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsFA_NET extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数

    /*
    networkmannerid: Int,	联网方式 id
networkmannername:String,	联网方式名称
     */
    val row = args(0).asInstanceOf[Row]

    val networkmannername = row.getAs[String]("networkmannername")

      if(networkmannername=="WIFI"){
        list :+= (" D00020001",1)
      }else if(networkmannername=="4G"){
        list :+= (" D00020002",1)
      }else if(networkmannername=="3G"){
        list :+= (" D00020003",1)
      }else if(networkmannername=="2G"){
        list :+= (" D00020004",1)
      }else{
        list :+= (" D00020005",1)
      }
    list

  }
}
