package ru.imho.ddsmt

import scala.annotation.tailrec
import java.io.PrintWriter

/**
 * Created by skotlov on 10.11.2014.
 */
object App {

  def main(args: Array[String]): Unit = {
    val arg = parseArgs(args)
    val config = ConfigParser.parse(arg.configFile)

    val storage = new StorageImpl(arg.storageLocation)
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        storage.close()
      }
    })

    val gs = new GraphService(config.params, config.rules, storage)

    if (arg.dotExportFileName.isDefined)
      writeToFile(arg.dotExportFileName.get, gs.toDot)
    else
      gs.run(arg.concurrencyDegree)
  }

  private def parseArgs(args: Array[String]): Args = {
    var configFile = "ddsmtConfig.xml"
    var storageLocation = "/ddsmt-storage/storage"
    var concurrencyDegree = 1
    var dotExportFileName: Option[String] = None

    @tailrec
    def parse(a: List[String]): Unit = a match {
      case "-h" :: r =>
        println("-h | [-c <configFile>] [-s <storageLocation>] [-p <concurrencyDegree>] [-e <dotExportFileName>]")
        System.exit(1)
      case "-c" :: c :: r =>
        configFile = c
        parse(r)
      case "-s" :: s :: r =>
        storageLocation = s
        parse(r)
      case "-p" :: p :: r =>
        concurrencyDegree = p.toInt
        parse(r)
      case "-e" :: e :: r =>
        dotExportFileName = Some(e)
        parse(r)
      case Nil =>
        Unit
      case _ =>
        sys.error("Invalid command line")
    }

    parse(args.toList)

    if (concurrencyDegree < 1 || concurrencyDegree > 50)
      throw new IllegalArgumentException("The range of allowed values of 'concurrencyDegree' - [1,50]")

    new Args(configFile, storageLocation, concurrencyDegree, dotExportFileName)
  }

  class Args (val configFile: String, val storageLocation: String, val concurrencyDegree: Int,
              val dotExportFileName: Option[String])

  private def writeToFile(fileName: String, text: String) {
    Utils.using(new PrintWriter(fileName)) {
      wr => wr.println(text)
    }
  }
}
