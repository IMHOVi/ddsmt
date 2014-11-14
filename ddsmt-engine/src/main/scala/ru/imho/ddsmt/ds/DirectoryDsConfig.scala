package ru.imho.ddsmt.ds

import ru.imho.ddsmt.params.Param

/**
 * Created by skotlov on 11/13/14.
 */
case class DirectoryDsConfig(path: String, checkStrategy: Option[CheckStrategies.Value]) extends DataSetConfig {

  override def createDataSetInstance(): DataSet = DirectoryDs(path)(this)

  override def createDataSetInstance(param: Param, paramName: String): DataSet = DirectoryDs(param.applyToString(path, paramName))(this)
}
