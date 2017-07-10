package com.softwareag.studies.sql

import com.softwareag.studies.loganalysis.LogEntry
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by VST on 10-07-2017.
  */
object LogSQL {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("BasicSQL").setMaster("local[2]")
    val spCx = new SparkContext(config)
    val sqlCx = new SQLContext(spCx)

    val rawLogsRDD = spCx.textFile("U:\\IntelliJ IDEA 2016.2.5\\.IntelliJIdea\\system\\tomcat\\Unnamed_ECP_C_ECP\\logs\\ecp.log.txt")
    val logsRDD = rawLogsRDD.filter(l => (l.length - l.replace("|", "").length) == 7)
    val tenantLogsRDD = logsRDD.map(_.split('|')).map(l => LogEntry(l(0), l(1), l(2), l(3), l(5).toLong, l(6), l(7)))

    val sqlTenantLogs = sqlCx.createDataFrame(tenantLogsRDD)
    sqlTenantLogs.createOrReplaceTempView("logs")
    val agg = sqlCx.sql("select tenant, count(tenant) as cnt from logs where tenant <> '' group by tenant order by cnt")
    println(agg.show())
   /* val level = sqlCx.sql("select tenant, level, count(tenant, level) as cnt from logs where tenant <> '' group by tenant, level order by cnt")
    println(level.show())*/
  }
}
