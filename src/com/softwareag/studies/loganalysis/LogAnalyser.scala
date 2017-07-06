package com.softwareag.studies.loganalysis

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by VST on 30-06-2017.
  */
object LogAnalyser {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[2]").setAppName("LogAnalyser")
    val context = new SparkContext(config)

    val rawLogsRDD = context.textFile("U:\\IntelliJ IDEA 2016.2.5\\.IntelliJIdea\\system\\tomcat\\Unnamed_ECP_C_ECP\\logs\\ecp.log.txt")
    val logsRDD = rawLogsRDD.filter(l => (l.length - l.replace("|", "").length) == 7)
    val tenantLogsRDD = logsRDD.map(_.split('|'))
      .map(l => (l(3), LogEntry(l(0), l(1), l(2), l(3), l(5).toLong, l(6), l(7))))
    println("Partition size ::" + tenantLogsRDD.partitions.size)
    val tenantLogsMapRDD = tenantLogsRDD.groupByKey()

    //See the amount logs generated for each tenant
    val tenantLogCountArr = tenantLogsMapRDD.map(tl => (tl._1, tl._2.size)).collect()
    tenantLogCountArr.foreach(tuple => {
      println(tuple._1 + "=======>" + tuple._2)
    })

    //See the amount of logs generated for each tenant with respect to the log level
    val tenantLogLevelRDD = tenantLogsRDD.map(le => ((le._1, le._2.level), 1)).reduceByKey((v1, v2) => v1 +v2)
    tenantLogLevelRDD.collectAsMap()map(println(_))
  }
}
