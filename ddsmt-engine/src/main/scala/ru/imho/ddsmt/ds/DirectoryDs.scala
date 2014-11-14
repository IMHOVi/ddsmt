package ru.imho.ddsmt.ds

import java.sql.Timestamp
import java.io.File
import scala.annotation.tailrec

/**
 * Created by skotlov on 11/13/14.
 */
case class DirectoryDs(path: String)(dsc: DataSetConfig) extends DataSet { // todo redesign param set

  def value = path
  def dataSetConfig: DataSetConfig = dsc

  override def checksum: String = {
    "todo" // todo
    /*
    final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
    messageDigest.reset();
    messageDigest.update(string.getBytes(Charset.forName("UTF8")));
    final byte[] resultByte = messageDigest.digest();
    final String result = new String(Hex.encodeHex(resultByte));

     */
  }

  override def endTimestamp: Timestamp = {
    val dir = new File(path)
    var max = 0L
    traverseAllFiles(dir, f => { max = Math.max(max, f.lastModified()) })
    new Timestamp(max)
  }

  override def startTimestamp: Timestamp = endTimestamp // todo: startTimestamp != endTimestamp

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
