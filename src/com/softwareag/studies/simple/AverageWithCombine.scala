package com.softwareag.studies.simple

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by VST on 04-07-2017.
  */
object AverageWithCombine {

  //How pink value is 1.4 instead of 3.5?
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("AverageWithCombine")
    val cx = new SparkContext(sc)
    val numRdd = cx.parallelize(Array(("panda",0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)))
    val avg = numRdd.combineByKey(
      (init) => (init, 1),
      (acc:(Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1:(Int, Int), acc2:(Int, Int)) => (acc1._1 + acc2._1, acc2._1 + acc2._2)
    ).map{case (k, v) => (k, v._1/v._2.toFloat)}
    avg.collectAsMap().map(println(_))
  }

}
