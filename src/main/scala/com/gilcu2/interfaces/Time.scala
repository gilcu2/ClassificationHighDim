package com.gilcu2.interfaces

import com.github.nscala_time.time.Imports._
import org.joda.time.format.PeriodFormat

object Time {

  val compactFormatter = DateTimeFormat.forPattern("yyyyMMdd-HHmm")

  def getCurrentTime: DateTime = DateTime.now

  def getHumanDuration(begin: DateTime, end: DateTime): String = {
    val totalTime = getDuration(begin, end)
    formatForHuman(totalTime)
  }

  def getDuration(begin: DateTime, end: DateTime): Duration = {
    val totalTime = (begin to end).toDuration()
    totalTime
  }

  def formatForHuman(totalTime: Duration): String =
    PeriodFormat.getDefault.print(totalTime.toPeriod)

  def formatCompatUTC(time: DateTime): String =
    time.withZone(DateTimeZone.UTC).toString(compactFormatter)
}
