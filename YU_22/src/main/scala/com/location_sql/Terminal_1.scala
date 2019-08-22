package com.location_sql

import org.apache.spark.sql.SparkSession

object Terminal_1 {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getName)
        .enableHiveSupport().getOrCreate()
      val df = spark.read.parquet("C:\\Users\\Administrator\\Desktop\\资料\\spark文档\\Spark用户画像分析\\res1")
      df.createOrReplaceTempView("DataTmp")

    //运营
      val res1 = spark.sql("select\nispname,\nsum(case when requestmode=1 and processnode>=1 then 1 else 0 end) originalrequests,\nsum(case when requestmode=1 and processnode>=2 then 1 else 0 end) validrequests,\nsum(case when requestmode=1 and processnode=3 then 1 else 0 end) ADrequests,\nsum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) Bidparticipation,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) Successfulbid,\nsum(case when requestmode=2 and iseffective=1 then 1 else 0 end) Showthenumber,\nsum(case when requestmode=3 and iseffective=1 then 1 else 0 end) hits,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) DSPADconsume,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) DSPADcost\nfrom\nDataTmp\ngroup by ispname,ispid")
      res1.show()
    //网络类
    val res2 = spark.sql("select\nnetworkmannername,\nsum(case when requestmode=1 and processnode>=1 then 1 else 0 end) originalrequests,\nsum(case when requestmode=1 and processnode>=2 then 1 else 0 end) validrequests,\nsum(case when requestmode=1 and processnode=3 then 1 else 0 end) ADrequests,\nsum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) Bidparticipation,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) Successfulbid,\nsum(case when requestmode=2 and iseffective=1 then 1 else 0 end) Showthenumber,\nsum(case when requestmode=3 and iseffective=1 then 1 else 0 end) hits,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) DSPADconsume,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) DSPADcost\nfrom\nDataTmp\ngroup by networkmannername,networkmannerid")
    res2.show()
//设备类
    val res3 = spark.sql("select\nmax(case devicetype when 1 then '手机' when 2 then '平板' else '其他'  end) devicetype,\nsum(case when requestmode=1 and processnode>=1 then 1 else 0 end) originalrequests,\nsum(case when requestmode=1 and processnode>=2 then 1 else 0 end) validrequests,\nsum(case when requestmode=1 and processnode=3 then 1 else 0 end) ADrequests,\nsum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) Bidparticipation,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) Successfulbid,\nsum(case when requestmode=2 and iseffective=1 then 1 else 0 end) Showthenumber,\nsum(case when requestmode=3 and iseffective=1 then 1 else 0 end) hits,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) DSPADconsume,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) DSPADcost\nfrom\nDataTmp\ngroup by devicetype")
    res3.show()
//操作系
    val res4 = spark.sql("select\nmax(case client when 1 then 'android' when 2 then 'ios' when 3 then 'wp' else '其他' end) client,\nsum(case when requestmode=1 and processnode>=1 then 1 else 0 end) originalrequests,\nsum(case when requestmode=1 and processnode>=2 then 1 else 0 end) validrequests,\nsum(case when requestmode=1 and processnode=3 then 1 else 0 end) ADrequests,\nsum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) Bidparticipation,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) Successfulbid,\nsum(case when requestmode=2 and iseffective=1 then 1 else 0 end) Showthenumber,\nsum(case when requestmode=3 and iseffective=1 then 1 else 0 end) hits,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) DSPADconsume,\nsum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) DSPADcost\nfrom\nDataTmp\ngroup by client")
    res4.show()


  }
}
