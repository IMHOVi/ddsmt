package ru.imho.ddsmt.commands

import ru.imho.ddsmt.ds.DataSet
import ru.imho.ddsmt.params.Param

/**
 * Created by skotlov on 11/13/14.
 */

trait CommandConfig {
  def createCommand(): Command

  def createCommand(param: Param, paramName: String): Command
}

trait Command {
  def execute(input: DataSet, output: DataSet)
}
