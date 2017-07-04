package com.softwareag.studies.loganalysis

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by VST on 30-06-2017.
  */
object LogAnalyser {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[2]").setAppName("LogAnalyser")
    val context = new SparkContext(config)

    val rawLogsRDD = context.textFile("C:\\SoftwareAG\\ARIS10.0\\server\\bin\\work\\work_ecp_m\\base\\logs\\ecp.log.txt.1")
    val logsRDD = rawLogsRDD.filter(l => (l.length - l.replace("|", "").length) == 7)
    val tenantLogsMapRDD = logsRDD.map(_.split('|'))
      .map(l => (l(3), LogEntry(l(0), l(1), l(2), l(3), l(5).toLong, l(6), l(7))))
      .groupByKey()

    //See the amount logs generated for each tenant
    val tenantLogCountArr = tenantLogsMapRDD.map(tl => (tl._1, tl._2.size)).collect()
    tenantLogCountArr.foreach(tuple => {
      println(tuple._1 + "=======>" + tuple._2)
    })

    //See the amount of logs generated for each tenant with respect to the log level
    //--------------------------------------{(tenant, [level, level, level])}----------------------------
    val tenantLogLevelRDD = tenantLogsMapRDD.map(tenLog => (tenLog._1, tenLog._2.foreach(le => le.level))).mapValues(l => (l, 1)).groupByKey()
    tenantLogLevelRDD.foreach(tll=>(tll._1, tll._2))
  }
}
