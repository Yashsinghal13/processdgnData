package com.octro.utilities

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import java.text.SimpleDateFormat
import scala.util.Try

object Format {

  final val dayFmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
  final val hourFmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH");
  final val fmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
  final val nikFmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd");
  final val fileFmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss");
  final val plrmyfmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd");
  final val pdtZone:DateTimeZone = DateTimeZone.forID("America/Los_Angeles")
  final val tZone:DateTimeZone = DateTimeZone.forID("UTC")  
  final val istZone:DateTimeZone = DateTimeZone.forID("Asia/Kolkata")
  final val utcZone:DateTimeZone = DateTimeZone.forID("UTC")

  
  def epochToDateTime(epochMillis: Long): String = {
    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(epochMillis)
  }
  
  def convertUtcToISTZone(dateTime: DateTime) =
    Try(dateTime.withZone(DateTimeZone.forID(Format.istZone.getID))) getOrElse dateTime.withZone(DateTimeZone.forID(Format.istZone.getID))
    
  def convertUtcToPDTZone(dateTime: DateTime) =
    Try(dateTime.withZone(DateTimeZone.forID(Format.pdtZone.getID))) getOrElse dateTime.withZone(DateTimeZone.forID(Format.pdtZone.getID))
    
}