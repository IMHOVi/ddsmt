package ru.imho.ddsmt.params

import org.quartz.CronExpression
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import org.joda.time.DateTime
import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 11/13/14.
 */
case class Hour(year: Int, month: Int, day: Int, hour: Int, prev: Option[Hour]) extends Param {

  override def applyToString(str: String, paramName: String): String = {
    // todo implement something better
    if(str.contains("${"+ paramName +".prev") && prev.isEmpty) {
      throw new ApplyParamException
    }

    val r = str.replace("${"+ paramName +"}", toString)
      .replace("${"+ paramName +".year}", year.toString)
      .replace("${"+ paramName +".month}", month.toString)
      .replace("${"+ paramName +".day}", day.toString)
      .replace("${"+ paramName +".hour}", hour.toString)

    val r2 = if (r.contains("${"+ paramName +".prev")) {
      r.replace("${"+ paramName +".prev}", prev.get.toString)
        .replace("${"+ paramName +".prev.year}", prev.get.year.toString)
        .replace("${"+ paramName +".prev.month}", prev.get.month.toString)
        .replace("${"+ paramName +".prev.day}", prev.get.day.toString)
        .replace("${"+ paramName +".prev.hour}", prev.get.hour.toString)
    } else {
      r
    }

    if (r2.contains("${")) {
      throw new RuntimeException("some ${param} was not replaced: " + r2)
    }
    r2
  }

  override def toString = {
    "%d-%d-%d-%d".format(year, month, day, hour)
  }
}

object Hour {

  def apply(cronExpression: String): Iterable[Hour] = {
    val cex = new CronExpression(cronExpression)
    val hours = ArrayBuffer[Hour]()
    var date = cex.getNextValidTimeAfter(new Date(0))
    var prevHour: Option[Hour] = None

    while (date != null) {
      val dt = new DateTime(date.getTime)
      val hour = new Hour(dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth, dt.getHourOfDay, prevHour)
      hours += hour
      prevHour = Some(hour)
      date = cex.getNextValidTimeAfter(date)
    }

    hours.toIterable
  }
}
