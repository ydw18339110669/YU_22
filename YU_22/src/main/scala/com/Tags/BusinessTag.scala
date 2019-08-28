package com.Tags

import ch.hsr.geohash.GeoHash
import com.ETL.Utils2Type
import com.utils.{AmapUtil, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.{Jedis, JedisPool}

object BusinessTag extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    val long = Utils2Type.toDouble(row.getAs[String]("long"))
    val lat = Utils2Type.toDouble(row.getAs[String]("lat"))
    //获取经纬度
    if(long>=73.0&&long<=135&&lat>=3.0&&lat<=54.0){
      val bussiness = getBusiness(long,lat)

      if(StringUtils.isNotBlank(bussiness)){
        val lines = bussiness.split(",")
        lines.foreach(f =>{list:+=(f,1)})
      }
    }
    list
  }

  def getBusiness(long:Double,lat:Double): String ={
    //转换GeoHash字符串
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)

    //检查缓存中是否有数据
    var business = redis_queryBusiness(geohash)

    if(business ==null || business.length == 0){
      business = AmapUtil.getBusinessFromAmap(long,lat)
      redis_insertBusiness(geohash,business)
    }

    business
  }

  /**
    * 在redis中获取商圈信息
    */
  def redis_queryBusiness(geohash:String)={
    val pool = new JedisPool("192.168.153.150")
    val jedis = pool.getResource
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 存入缓存
    * @param geohash
    * @param business
    */
  def redis_insertBusiness(geohash:String,business:String)={
    val pool = new JedisPool("192.168.153.150")
    val jedis = pool.getResource
    jedis.set(geohash,business)
    jedis.close()
  }




}
