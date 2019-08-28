package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsFA_ISP extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //解析参数

    /*
  ispid: Int,	运营商 id
ispname: String,	运营商名称
     */
    val row = args(0).asInstanceOf[Row]


    val ispname = row.getAs[String]("ispname")


    if(ispname=="移动"){
      list :+= (" D00030001",1)
    }else if(ispname=="联通"){
      list :+= (" D00030002",1)
    }else if(ispname=="电信"){
      list :+= (" D00030003",1)
    }else{
      list :+= (" D00030004",1)
    }
    list

  }
}
