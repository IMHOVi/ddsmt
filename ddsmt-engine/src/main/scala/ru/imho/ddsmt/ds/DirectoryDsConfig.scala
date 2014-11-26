package ru.imho.ddsmt.ds

import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 11/13/14.
 */
class DirectoryDsConfig(val path: String, val checkStrategy: Option[CheckStrategies.Value]) extends DataSetConfig {

  override def createDataSetInstance(): DataSet = new DirectoryDs(path, this)

  override def createDataSetInstance(param: Param, paramName: String): DataSet =
    new DirectoryDs(param.applyToString(path, paramName), this)
}
