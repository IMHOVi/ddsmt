package ru.imho.ddsmt.commands

import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 11/13/14.
 */
class ExtCommandConfig(commands: Iterable[String], policy: CommandPolicy) extends CommandConfig {

  override def createCommand(): Command = new ExtCommand(commands, policy)

  override def createCommand(param: Param, paramName: String): Command =
    new ExtCommand(commands.map(c => param.applyToString(c, paramName)), policy)
}
