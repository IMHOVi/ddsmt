package ru.imho.ddsmt.ds

import org.apache.commons.net.ftp.{FTPReply, FTPClient, FTPFile}
import java.io.IOException
import ru.imho.ddsmt.Base._
import java.sql.Timestamp
import ru.imho.ddsmt.Logger

/**
 * Created by skotlov on 11/18/14.
 */
class FtpDs(hostname: String, username: Option[String], password: Option[String], path: String, fileNameRegex: Option[String], dsc: DataSetConfig) extends DataSet {

  override def id: String = "Ftp(hostname: %s, path: %s, files: %s)".format(hostname, path, fileNameRegex.getOrElse("all"))

  override def dataSetConfig: DataSetConfig = dsc

  override def checksum: Option[String] = throw new UnsupportedOperationException("FtpDs doesn't support checksum")

  override def timestamp: Option[Timestamp] = {
    var max = 0L
    traverseAllFtpFiles(hostname, username, password, path, fileNameRegex, f => { max = Math.max(max, f.getTimestamp.getTimeInMillis) })
    if (max == 0L) None else Some(new Timestamp(max))
  }

  override def displayName: String = id

  private def traverseAllFtpFiles(hostname: String, username: Option[String], password: Option[String], path: String, fileNameRegex: Option[String], h: FTPFile => Unit): Unit = {
    val ftp = new FTPClient()
    try {
      ftp.connect(hostname)

      if(!ftp.login(username.getOrElse(""), password.getOrElse(""))) {
        throw new RuntimeException("Cannot login to " + hostname)
      }

      if (!FTPReply.isPositiveCompletion(ftp.getReplyCode)) {
        throw new RuntimeException("Cannot successfully connect to " + hostname)
      }

      ftp.enterLocalPassiveMode()
      traverseAllFtpFiles(ftp, path, fileNameRegex, h)
    } finally {
      try {
        ftp.logout()
      } catch {
        case e: IOException => Logger.error("Cannot logout from ftp - %s. Error - %s".format(displayName, e))
      }

      try {
        ftp.disconnect()
      } catch {
        case e: IOException => Logger.error("Cannot disconnect from ftp - %s. Error - %s".format(displayName, e))
      }
    }
  }

  private def traverseAllFtpFiles(ftp: FTPClient, path: String, fileNameRegex: Option[String], h: FTPFile => Unit) {
    val p = if(path.endsWith("/")) path else path + "/"
    ftp.listFiles(p)
      .filter(_.isFile)
      .filter(f => fileNameRegex.isEmpty || f.getName.matches(fileNameRegex.get))
      .foreach(f => h(f))
    ftp.listDirectories(p).foreach(d => traverseAllFtpFiles(ftp, p + d.getName, fileNameRegex, h))
  }
}
