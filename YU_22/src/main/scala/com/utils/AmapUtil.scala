package com.utils

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  *
  */
object AmapUtil {
// 获取高德地图商圈信息
  def getBusinessFromAmap(long:Double,lat:Double):String={
    val location = long+","+lat
    val urlStr ="https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=980418d5ff198094a6e8140549b8a173"
    val jsonstr = HttpUtil.get(urlStr)
    //解析json串
    val jsonparse = JSON.parseObject(jsonstr)
    //判断是否获取成功
    val status = jsonparse.getIntValue("status")
    if(status ==0) return ""
    //接下来解析内部json串，判断每个key的value都不能为空
    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val addressComponentJson = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    val businessAreasArray = addressComponentJson.getJSONArray("businessAreas")
    if(businessAreasArray == null || businessAreasArray.isEmpty) return null

    //创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    //循环输出
    for(item <- businessAreasArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }


}
