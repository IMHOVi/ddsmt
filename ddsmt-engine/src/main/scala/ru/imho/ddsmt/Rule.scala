package ru.imho.ddsmt

import ru.imho.ddsmt.Base._
import scala.collection.mutable.ArrayBuffer
import java.sql.Timestamp

/**
  * Created by skotlov on 11/13/14.
  */
case class Rule(name: String, input: Iterable[DataSet], output: Iterable[DataSet], cmd: Command)(storage: Storage) extends Node {

  val displayName = "Rule %s (Input - [%s], Output - [%s])".format(name, input.map(_.displayName).mkString(", "), output.map(_.displayName).mkString(", "))

  @volatile
  var executed = false

  def execute(): Unit = {
    if (executed)
      throw new RuntimeException(displayName + " already was executed")
    else
      executed = true

    try {
      val changedTs = ArrayBuffer[(String, Option[Timestamp])]()
      val changedCs = ArrayBuffer[(String, Option[String])]()

      if (input.map(i => isChanged(i, changedTs, changedCs)).exists(i => i)) {
        Logger.info(displayName + " requires execution...")
        cmd.execute(this)
        Logger.info(displayName + " was executed successfully.")
      } else {
        Logger.info(displayName + " doesn't require execution. No input DataSets was changed.")
      }

      changedTs.foreach(ts => storage.setLastKnownTimestamp(ts._1, name)(ts._2))
      changedCs.foreach(cs => storage.setLastKnownChecksum(cs._1, name)(cs._2))
    } catch {
      case e: Throwable => {
        Logger.error(displayName + " was executed with error: " + e.getMessage)
        throw e
      }
    }
  }

  private def isChanged(input: DataSet, changedTs: ArrayBuffer[(String, Option[Timestamp])], changedCs: ArrayBuffer[(String, Option[String])]): Boolean = {
    val cs = if (input.dataSetConfig.checkStrategy.isDefined)
      input.dataSetConfig.checkStrategy.get
    else
      return true

    def requiredByTimestamp = {
      val lastKn = storage.getLastKnownTimestamp(input.id, name)
      val inTs = input.timestamp
      if (lastKn != inTs) {
        changedTs += ((input.id, inTs))
      }

      if (inTs.isDefined && lastKn.isDefined)
        inTs.get.getTime != lastKn.get.getTime
      else
        true
    }

    def requiredByChecksum = {
      val lastKn = storage.getLastKnownChecksum(input.id, name)
      val inCs = input.checksum
      if (lastKn != inCs) {
        changedCs += ((input.id, inCs))
      }

      if (inCs.isDefined && lastKn.isDefined)
        inCs.get != lastKn.get
      else
        true
    }

    if (cs == CheckStrategies.timestamp) {
      requiredByTimestamp
    } else if (cs == CheckStrategies.timestampChecksum) {
      if (requiredByTimestamp) {
        true
      } else {
        requiredByChecksum
      }
    } else if (cs == CheckStrategies.checksum) {
      requiredByChecksum
    } else {
      throw new RuntimeException("unsupported CheckStrategy: " + cs)
    }
  }
}
