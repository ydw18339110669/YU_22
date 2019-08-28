package com.Tags
import com.utils.Tag
import org.apache.spark.sql.Row

object TagsCN extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    //空值判断
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    if (adplatformproviderid >= 100000) {
      list :+= ("CN" + adplatformproviderid, 1)
    }
    list
  }
}
