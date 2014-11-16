package ru.imho.ddsmt

import ru.imho.ddsmt.Base.Storage
import java.io.Closeable
import jdbm.RecordManagerFactory
import java.sql.Timestamp

/**
 * Created by skotlov on 11/14/14.
 */
class StorageImpl(fileName: String) extends Storage with Closeable {
  // todo(postpone): 1) cleanup storage? 2) consider move to h2?

  val recordManager = RecordManagerFactory.createRecordManager(fileName)
  val lastKnownChecksums = recordManager.hashMap[String, Option[String]]("lastKnownChecksums")
  val lastKnownTimestamps = recordManager.hashMap[String, Option[Timestamp]]("lastKnownTimestamps")

  override def getLastKnownChecksum(dataSetId: String, ruleName: String): Option[String] = {
    lastKnownChecksums.get(key(dataSetId, ruleName)) match {
      case null => None
      case v => v
    }
  }

  override def setLastKnownChecksum(dataSetId: String, ruleName: String)(checksum: Option[String]): Unit = {
    lastKnownChecksums.put(key(dataSetId, ruleName), checksum)
    recordManager.commit()
  }

  override def getLastKnownTimestamp(dataSetId: String, ruleName: String): Option[Timestamp] = {
    lastKnownTimestamps.get(key(dataSetId, ruleName)) match {
      case null => None
      case v => v
    }
  }

  override def setLastKnownTimestamp(dataSetId: String, ruleName: String)(timestamp: Option[Timestamp]): Unit = {
    lastKnownTimestamps.put(key(dataSetId, ruleName), timestamp)
    recordManager.commit()
  }

  private def key(dataSetId: String, ruleName: String) = "ds:%s::rn:%s".format(dataSetId, ruleName)

  override def close() {
    recordManager.close()
  }
}
