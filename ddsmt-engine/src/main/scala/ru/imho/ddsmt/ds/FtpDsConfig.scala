package ru.imho.ddsmt.ds

import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 11/19/14.
 */
class FtpDsConfig(val hostname: String, val username: Option[String], val password: Option[String],
                  val path: String, val fileNameRegex: Option[String],
                  val checkStrategy: Option[CheckStrategies.Value]) extends DataSetConfig {
  require(checkStrategy.isEmpty || checkStrategy.get == CheckStrategies.timestamp, "FtpDs supports only 'timestamp' CheckStrategy")

  override def createDataSetInstance(): DataSet = new FtpDs(hostname, username, password, path, fileNameRegex, this)

  override def createDataSetInstance(param: Param, paramName: String): DataSet =
    new FtpDs(param.applyToString(hostname, paramName),
      param.applyToString(username, paramName),
      param.applyToString(password, paramName),
      param.applyToString(path, paramName),
      param.applyToString(fileNameRegex, paramName),
      this)
}
