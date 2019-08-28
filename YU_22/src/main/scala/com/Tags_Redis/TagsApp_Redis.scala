package com.Tags_Redis

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object TagsApp_Redis extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]

    val jedis = new Jedis("192.168.153.150", 6379)

    val appname = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")
    //空值判断
    if(StringUtils.isNotBlank(appname)){
      list :+=("APP"+appname,1)
    }else if(StringUtils.isNotBlank(appid)){
      if(StringUtils.isNotBlank(jedis.get(appid))){
        list :+= ("APP"+jedis.get(appid),1)
      }else{
        list :+= ("APP其他",1)
      }
    }
    list
  }
}
