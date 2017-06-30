package com.softwareag.studies.dataframes

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by VST on 28-06-2017.
  */
object ScalaDataFrames {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[2]").setAppName("ScalaDataFrames")
    val sc = new SparkContext(config)
    val sqlCx = new SQLContext(sc)

    import sqlCx.implicits._
    import org.apache.spark.sql._

    val ebayText = sc.textFile("Cartier+for+WinnersCurse.csv")
    val ebay = ebayText.map(_.split(",")).map(p => Auction(p(0), p(1).toFloat, p(2).toFloat, p(3), p(4).toInt, p(5).toFloat, p(6).toFloat))
    val df = ebay.toDF()
    /*df.show()
    df.printSchema()*/
   /* println(df.select("auctionid").distinct().count())

    val groupsBy = df.groupBy("auctionid")
    println(groupsBy.min("price").show())
    println(groupsBy.max("price").show())
    println(groupsBy.avg("price").show())*/

    df.registerTempTable("auction")
    /*val query = sqlCx.sql("select auctionid, max(price), min(price), avg(price), count(auctionid) from auction group by auctionid")
    println(query.show())*/

    println(sqlCx.sql("select * from auction where auctionid='1641880134'").show())
  }
}
