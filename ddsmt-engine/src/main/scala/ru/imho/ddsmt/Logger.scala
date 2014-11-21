package ru.imho.ddsmt

import java.text.SimpleDateFormat
import java.util.Date
import org.fusesource.jansi.AnsiConsole
import org.fusesource.jansi.Ansi._
import org.fusesource.jansi.Ansi.Color._

/**
 * Created by skotlov on 11/17/14.
 */
object Logger {

  private val dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss")
  AnsiConsole.systemInstall()

  def info(s: String) {
    System.out.println(ansi().fg(GREEN).a(time + ": " + s).reset())
  }

  def warning(s: String) {
    System.out.println(ansi().fg(YELLOW).a(time + ": " + s).reset())
  }

  def error(s: String) {
    System.err.println(ansi().fg(RED).a(time + ": " + s).reset())
  }

  private def time = dateFormat.format(new Date())
}
