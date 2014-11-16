package ru.imho.ddsmt

import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 11/13/14.
 */
class RuleConfig(val name: String, val param: Option[ParamDesc], val input: Iterable[DataSetConfig], val output: Iterable[DataSetConfig], val cmd: CommandConfig)

case class ParamDesc(paramName: String, paramPolicy: ParamPolicy.Value)

object ParamPolicy extends Enumeration {
  val forEach = Value(0, "forEach")
}
