package com.softwareag.studies.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by VST on 10-07-2017.
  */
object BasicSQL {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("BasicSQL").setMaster("local[2]")
    val spCx = new SparkContext(config)
    val sqlCx = new SQLContext(spCx)

    import sqlCx.implicits._

    val pinCode = sqlCx.jsonFile("data/zips.json")
    pinCode.registerTempTable("pincode")
    pinCode.printSchema()

    val logs = sqlCx.sql("select city from pincode where pop > 40000 and pop < 50000")
    val cities = logs.map(r => r.getString(0))
    println(cities.collectAsList())
  }

}
