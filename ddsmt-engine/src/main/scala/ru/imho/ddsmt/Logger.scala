package ru.imho.ddsmt

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by skotlov on 11/17/14.
 */
object Logger {

  private val dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss")

  def info(s: String) {
    System.out.println(time + ": " + s)
  }

  def error(s: String) {
    System.err.println(time + ": " + s)
  }

  private def time = dateFormat.format(new Date())
}
