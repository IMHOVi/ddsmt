package ru.imho.ddsmt.commands

import ru.imho.ddsmt.Base._
import ru.imho.ddsmt.{Logger, Rule}

/**
 * Created by skotlov on 11/13/14.
 */
class ExtCommand(commands: Iterable[String], val policy: CommandPolicy) extends Command {

  override def execute(rule: Rule): Unit = {
    import scala.collection.JavaConversions._
    val pb = new ProcessBuilder(commands.toList)
    val p = pb.start()

    val destroyProcess = policy.maxExecutionTime match {
      case None => None
      case Some(d) => Some(rule.scheduler.scheduleOnce(d){
        Logger.error(rule.displayName + " exceeded the max execution time=(" + d + ") and will be aborted")
        p.destroy()
      })
    }

    val r = p.waitFor()

    if (destroyProcess.isDefined && !destroyProcess.get.isCancelled)
      destroyProcess.get.cancel()

    if (r != 0) {
      throw new RuntimeException("Ext command (%s) was executed with error. ErrorCode - %d".format(commands.mkString(", "), r))
    }
  }
}
