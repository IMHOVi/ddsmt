package ru.imho.ddsmt.ds

import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 11/13/14.
 */
class GenericDsConfig(id: String) extends DataSetConfig {

  override def createDataSetInstance(): DataSet = new GenericDs(id, this)

  override def createDataSetInstance(param: Param, paramName: String): DataSet =
    new GenericDs(param.applyToString(id, paramName), this)

  override def checkStrategy: Option[CheckStrategies.Value] = None
}
