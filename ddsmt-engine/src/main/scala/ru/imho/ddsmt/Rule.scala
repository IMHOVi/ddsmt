package ru.imho.ddsmt

import ru.imho.ddsmt.Base._

/**
  * Created by skotlov on 11/13/14.
  */
case class Rule(name: String, input: Iterable[DataSet], output: Iterable[DataSet], cmd: Command)(storage: Storage) extends Node {

  def execute(): Unit = {
    if (input.exists(i => isChanged(i))) {
      cmd.execute(input, output)
    }
  }

  private def isChanged(input: DataSet): Boolean = {
    val cs = if (input.dataSetConfig.checkStrategy.isDefined)
      input.dataSetConfig.checkStrategy.get
    else
      return true

    def requiredByTimestamp = {
      val lastKn = storage.getLastKnownTimestamp(input.id, name)
      val inTs = input.timestamp
      if (lastKn != inTs) {
        storage.setLastKnownTimestamp(input.id, name)(inTs)
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
        storage.setLastKnownChecksum(input.id, name)(inCs)
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
