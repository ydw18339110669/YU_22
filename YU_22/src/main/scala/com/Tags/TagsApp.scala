package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsApp extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
    val appMap: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    //空值判断
    if(StringUtils.isNotBlank(appname)){
      list :+=("APP"+appname,1)
    }else if(StringUtils.isNotBlank(appid)){
      list :+=("APP"+appMap.value.getOrElse(appid,"其他"),1)
    }
    list
  }
}
