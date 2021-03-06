package ru.imho.ddsmt

import java.sql.Timestamp
import scala.concurrent.duration.FiniteDuration

/**
 * Created by skotlov on 11/14/14.
 */
object Base {

  trait Node

  // DataSet

  trait DataSet extends Node {

    def id: String

    def dataSetConfig: DataSetConfig

    def timestamp: Option[Timestamp]

    def checksum: Option[String]

    def canEqual(other: Any): Boolean = other.isInstanceOf[DataSet]

    override def equals(other: Any): Boolean = other match {
      case that: DataSet =>
        (that canEqual this) &&
          id == that.id
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(id)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }

    def displayName: String
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

  // Command

  trait CommandConfig {
    def createCommand(): Command

    def createCommand(param: Param, paramName: String): Command
  }

  trait Command {

    def execute(rule: Rule)

    def policy: CommandPolicy
  }

  class CommandPolicy(
    val expectedExecutionTime: Option[FiniteDuration],
    val maxExecutionTime: Option[FiniteDuration]
  )

  // Param

  trait Param {

    def applyToString(str: String, paramName: String): String

    def applyToString(str: Option[String], paramName: String): Option[String] = str match {
      case None => None
      case Some(s) => Some(applyToString(s, paramName))
    }
  }

  // Storage

  trait Storage {

    def getLastKnownChecksum(dataSetId: String, ruleName: String): Option[String]

    def setLastKnownChecksum(dataSetId: String, ruleName: String)(checksum: Option[String])

    def getLastKnownTimestamp(dataSetId: String, ruleName: String): Option[Timestamp]

    def setLastKnownTimestamp(dataSetId: String, ruleName: String)(timestamp: Option[Timestamp])
  }
}
