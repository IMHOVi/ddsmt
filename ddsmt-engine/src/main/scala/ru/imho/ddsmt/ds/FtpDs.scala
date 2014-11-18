package ru.imho.ddsmt.ds

import org.apache.commons.net.ftp.{FTPReply, FTPClient, FTPFile}
import java.io.IOException

/**
 * Created by skotlov on 11/18/14.
 */
class FtpDs {

  def traverseAllFtpFiles(serverAddress: String, username: Option[String], password: Option[String], path: String, fileNameRegex: Option[String], h: FTPFile => Unit): Unit = {
    val ftp = new FTPClient()
    try {
      ftp.connect(serverAddress)

      if(!ftp.login(username.getOrElse(""), password.getOrElse(""))) {
          throw new RuntimeException("Cannot login to " + serverAddress)
      }

      if (!FTPReply.isPositiveCompletion(ftp.getReplyCode)) {
        throw new RuntimeException("Cannot successfully connect to " + serverAddress)
      }

      ftp.enterLocalPassiveMode()
      traverseAllFtpFiles(ftp, path, fileNameRegex, h)
    } finally {
      try {
        ftp.logout()
      } catch {
        case e: IOException => // todo log
      }

      try {
        ftp.disconnect()
      } catch {
        case e: IOException => // todo log
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
