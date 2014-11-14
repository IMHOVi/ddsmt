package ru.imho.ddsmt.ds

import ru.imho.ddsmt.params.Param

/**
 * Created by skotlov on 11/13/14.
 */
class GenericDsConfig(value: String) extends DataSetConfig {

  override def createDataSetInstance(): DataSet = new GenericDs(value)(this)

  override def createDataSetInstance(param: Param, paramName: String): DataSet = GenericDs(param.applyToString(value, paramName))(this)

  override def checkStrategy: Option[CheckStrategies.Value] = None
}
