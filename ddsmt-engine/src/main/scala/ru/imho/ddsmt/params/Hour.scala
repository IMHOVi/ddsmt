package ru.imho.ddsmt.params

import org.quartz.CronExpression
import java.util.{Calendar, Date}
import scala.collection.mutable.ArrayBuffer
import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 11/13/14.
 */
case class Hour(year: Int, month: Int, day: Int, hour: Int, prev: Option[Hour]) extends Param {

  override def applyToString(str: String, paramName: String): String = {
    // todo(postpone): rework parsing
    if (!str.contains("${")) {
      return str
    }

    if(str.contains("${"+ paramName +".prev") && prev.isEmpty) {
      throw new RuntimeException("Cannot apply %s to %s".format(this, str))
    }

    def leftPad(s: String) = if (s.length == 1) "0" + s else s // todo(postpone): it must be configured

    val r = str.replace("${"+ paramName +"}", toString)
      .replace("${"+ paramName +".year}", year.toString)
      .replace("${"+ paramName +".month}", leftPad(month.toString))
      .replace("${"+ paramName +".day}", leftPad(day.toString))
      .replace("${"+ paramName +".hour}", leftPad(hour.toString))

    val r2 = if (r.contains("${"+ paramName +".prev")) {
      r.replace("${"+ paramName +".prev}", prev.get.toString)
        .replace("${"+ paramName +".prev.year}", prev.get.year.toString)
        .replace("${"+ paramName +".prev.month}", leftPad(prev.get.month.toString))
        .replace("${"+ paramName +".prev.day}", leftPad(prev.get.day.toString))
        .replace("${"+ paramName +".prev.hour}", leftPad(prev.get.hour.toString))
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
      val c = Calendar.getInstance()
      c.setTime(date)
      val hour = new Hour(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH),
        c.get(Calendar.HOUR_OF_DAY), prevHour)
      hours += hour
      prevHour = Some(hour)
      date = cex.getNextValidTimeAfter(date)
    }

    hours.toIterable
  }
}
