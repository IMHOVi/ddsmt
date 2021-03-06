package ru.imho.ddsmt.ds

import java.sql.Timestamp
import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 11/13/14.
 */
class GenericDs(val id: String, dsc: DataSetConfig) extends DataSet {

  def dataSetConfig: DataSetConfig = dsc

  override def checksum: Option[String] = None

  override def timestamp: Option[Timestamp] = None

  def displayName: String = "DS(" + id + ")"
}
