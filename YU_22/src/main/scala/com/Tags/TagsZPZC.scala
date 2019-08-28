package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object TagsZPZC extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]

    val provincename = row.getAs[String]("provincename")
    if (StringUtils.isNoneBlank(provincename)) {
      list :+= ("ZP" + provincename, 1)
    }
    val cityname = row.getAs[String]("cityname")
    if (StringUtils.isNoneBlank(cityname)) {
      list :+= ("ZC" + cityname, 1)
    }
    list
  }
}
