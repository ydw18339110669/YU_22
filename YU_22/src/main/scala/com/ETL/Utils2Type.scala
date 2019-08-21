package com.ETL

object Utils2Type {
    def toInt(str:String)={

      try {
        str.toInt
      } catch {
        case _ :Exception => 0
      }



    }

  def toDouble(str:String)={

    try {
      str.toDouble
    } catch {
      case _ :Exception => 0.0
    }

  }
}
