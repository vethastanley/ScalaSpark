package com.softwareag.studies.loganalysis

/**
  * Created by VST on 30-06-2017.
  */
case class LogEntry(date:String, level:String, instance:String, tenant:String, requestCount:Long, threadName:String,
                    message:String)
