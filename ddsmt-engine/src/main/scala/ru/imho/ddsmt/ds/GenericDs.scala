package ru.imho.ddsmt.ds

import java.sql.Timestamp

/**
 * Created by skotlov on 11/13/14.
 */
case class GenericDs(v: String)(dsc: DataSetConfig) extends DataSet { // todo redesign param set

  def value = v
  def dataSetConfig: DataSetConfig = dsc

  override def checksum: String = throw new UnsupportedOperationException

  override def endTimestamp: Timestamp = throw new UnsupportedOperationException

  override def startTimestamp: Timestamp = throw new UnsupportedOperationException
}
