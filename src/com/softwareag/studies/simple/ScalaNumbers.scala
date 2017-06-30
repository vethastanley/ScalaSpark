package com.softwareag.studies.simple

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by VST on 28-06-2017.
  */
object ScalaNumbers {

  def main(args: Array[String]): Unit = {
    val nums = 1 to 50000
    val config = new SparkConf().setMaster("local[2]").setAppName("Number check")
    val context = new SparkContext(config)
    val rdd = context.parallelize(nums)
    val coll = rdd.filter(_ < 10).collect()
    coll.foreach(c => {
      println(c)
    })
  }

}
