package com.test

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 商圈解析工具
  */
object MyJsonUtil {
// 获取高德地图商圈信息
  def getBusinessFromAmap(logs:String):String={

     //定义json格式
    val jsonparse = JSON.parseObject(logs)

    //判断是否获取成功（考虑获取到数据的成功因素 status=1成功 starts=0 失败）
    val status = jsonparse.getIntValue("status")
    if(status ==0) return ""

    //接下来解析内部json串，判断每个key的value都不能为空
    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val poisArr = regeocodeJson.getJSONArray("pois")
    if(poisArr == null || poisArr.isEmpty) return ""

    //创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()

    //循环输出
    for(item <- poisArr.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("businessarea"))
      }
    }
    buffer.mkString(",")
  }


}
