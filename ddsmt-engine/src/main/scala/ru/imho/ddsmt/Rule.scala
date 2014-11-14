package ru.imho.ddsmt

import ru.imho.ddsmt.commands.Command
import ru.imho.ddsmt.ds.DataSet

/**
  * Created by skotlov on 11/13/14.
  */
case class Rule(input: DataSet, output: DataSet, cmd: Command) {

  def execute(): Unit = {
    // todo check checkStrategy
    cmd.execute(input, output)
  }
}
