package ru.imho.ddsmt

import ru.imho.ddsmt.Base._

/**
  * Created by skotlov on 11/13/14.
  */
case class Rule(name: String, input: DataSet, output: DataSet, cmd: Command)(storage: Storage) {

  def execute(): Unit = {
    if (requiredExecution) {
      cmd.execute(input, output)
    }
  }

  private def requiredExecution: Boolean = {
    val cs = if (input.dataSetConfig.checkStrategy.isDefined)
      input.dataSetConfig.checkStrategy.get
    else
      return true

    def requiredByTimestamp = {
      val inEnd = input.endTimestamp
      val outStart = output.startTimestamp
      if (inEnd.isDefined && outStart.isDefined)
        inEnd.get.getTime > outStart.get.getTime
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
