package ru.imho.ddsmt.ds

import java.sql.Timestamp
import java.io.{FileInputStream, File}
import java.security.MessageDigest
import ru.imho.ddsmt.Utils
import org.apache.commons.codec.binary.Hex
import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 11/13/14.
 */
class DirectoryDs(path: String, dsc: DataSetConfig) extends DataSet {

  def id = path

  def dataSetConfig: DataSetConfig = dsc

  override def checksum: Option[String] = {
    val md5 = MessageDigest.getInstance("MD5")
    val buffer = new Array[Byte](1024)

    traverseAllFiles(new File(path), f => {
      if (f.isFile) {
        Utils.using(new FileInputStream(f)) { is =>
          var numRead: Int = 0
          do {
            numRead = is.read(buffer)
            if (numRead > 0) {
              md5.update(buffer, 0, numRead)
            }
          } while (numRead != -1)
        }
      }
    })

    Some(Hex.encodeHexString(md5.digest()))
  }

  override def endTimestamp: Option[Timestamp] = {
    var max = 0L
    traverseAllFiles(new File(path), f => { max = Math.max(max, f.lastModified()) })
    Some(new Timestamp(max))
  }

  override def startTimestamp: Option[Timestamp] = endTimestamp // todo: startTimestamp != endTimestamp

  private def traverseAllFiles(dir: File, h: File => Unit) {
    if (dir.exists()) {
      h(dir)

      if (dir.isDirectory) {
        dir.listFiles().foreach(f => {
          if (f.isFile)
            h(f)
          else
            traverseAllFiles(f, h)
        })
      }
    }
  }
}
