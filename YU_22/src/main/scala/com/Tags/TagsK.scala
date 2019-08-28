package com.Tags
import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row


object TagsK extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

        //解析参数
        val row = args(0).asInstanceOf[Row]
        val stopwords = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]
    //keywords: String,	关键字
    val keywords: Array[String] = row.getAs[String]("keywords").split("\\|")
        keywords.filter(word =>{
          word.length>=3 && word.length<=8 && !stopwords.value.contains(word)
        }).foreach(word => list :+= ("K"+word,1))
        list
  }
}