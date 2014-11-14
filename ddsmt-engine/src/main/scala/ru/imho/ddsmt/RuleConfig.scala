package ru.imho.ddsmt

import ru.imho.ddsmt.commands.CommandConfig
import ru.imho.ddsmt.ds.DataSetConfig

/**
 * Created by skotlov on 11/13/14.
 */
class RuleConfig(val param: Option[ParamDesc], val input: DataSetConfig, val output: DataSetConfig, val cmd: CommandConfig)

case class ParamDesc(paramName: String, paramPolicy: ParamPolicy.Value)

object ParamPolicy extends Enumeration {
  val forEach = Value(0, "forEach")
}
