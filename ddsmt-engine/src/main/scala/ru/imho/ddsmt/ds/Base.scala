package ru.imho.ddsmt.ds

import ru.imho.ddsmt.params.Param
import java.sql.Timestamp

/**
 * Created by skotlov on 11/13/14.
 */

trait DataSet {

  def value: String

  def dataSetConfig: DataSetConfig

  def startTimestamp: Timestamp

  def endTimestamp: Timestamp

  def checksum: String
}

trait DataSetConfig {

  def createDataSetInstance(): DataSet

  def createDataSetInstance(param: Param, paramName: String): DataSet

  def checkStrategy: Option[CheckStrategies.Value]
}

object CheckStrategies extends Enumeration {
  val timestamp = Value(0, "timestamp")
  val timestampChecksum = Value(1, "timestampChecksum")
  val checksum = Value(2, "checksum")
}
