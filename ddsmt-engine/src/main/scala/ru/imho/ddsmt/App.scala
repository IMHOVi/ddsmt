package ru.imho.ddsmt

import ru.imho.ddsmt.params.Hour
import ru.imho.ddsmt.ds.{DirectoryDsConfig, GenericDsConfig}
import ru.imho.ddsmt.commands.ExtCommandConfig
import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 10.11.2014.
 */
object App {

  def main(args: Array[String]): Unit = {

    val paramFabric: Map[String, String => Iterable[Param]] = Map("Hour" -> (v => Hour(v)))

    // todo read from config
    val paramName = "hour"
    val paramType = "Hour"
    val paramValue = "0 0 0-5 1 NOV ? 2014"
    val params = Map(paramName -> paramFabric(paramType)(paramValue))

    val paramDesc = Some(ParamDesc("hour", ParamPolicy.forEach))

    val adFoxFtpDS = new GenericDsConfig("AdFoxFtp")
    val adFoxLocalDS = new DirectoryDsConfig("/data/temp/ddsmt/ad-fox-local/${hour}", Some(CheckStrategies.timestamp))
    val loadLogsFromAdFox = new RuleConfig("loadLogsFromAdFox", paramDesc, adFoxFtpDS, adFoxLocalDS, new ExtCommandConfig(Iterable("/data/temp/writeToFile.sh", "wget --limit-rate=3M ftp://imho-video:OltUjLeit1@ftp.adfox.ru/$hour.eve.gz >> " + adFoxLocalDS + " # Загрузка лог-файлов из Adfox по ftp 1/час")))

    val parquetAndPrevUniquesDs = new DirectoryDsConfig("/data/temp/ddsmt/parquetAndPrevUniques/${hour}", Some(CheckStrategies.timestamp))
    val convertXmlToParquet = new RuleConfig("convertXmlToParquet", paramDesc, adFoxLocalDS, parquetAndPrevUniquesDs, new ExtCommandConfig(Iterable("/data/temp/writeToFile.sh", "hadoop jar scalding-jobs.jar com.twitter.scalding.Tool ru.imho.bd.etl.TransformAdfox --hdfs --input " + adFoxLocalDS + " --output " + parquetAndPrevUniquesDs + "/parquet # Конвертация файлов из pseudo-XML в Parquet\t1/час")))

    val vvAndUniquesDs =  new DirectoryDsConfig("/data/temp/ddsmt/vvAndUniques/${hour}", Some(CheckStrategies.timestamp))
    val createVv = new RuleConfig("createVv", paramDesc, parquetAndPrevUniquesDs, vvAndUniquesDs, new ExtCommandConfig(Iterable("/data/temp/writeToFile.sh", "hadoop jar scalding-fat-1.0.jar com.twitter.scalding.Tool -libjars scalding-jobs-1.0.21-SNAPSHOT.jar ru.imho.bd.vidaview.VVReports --hdfs --inputHour " + parquetAndPrevUniquesDs + "/parquet --output " + vvAndUniquesDs + " --prevUnique " + parquetAndPrevUniquesDs + "/unique # Построение VV dataset из Parquet и uniques предыдущего часа 1/час")))

    val prevVvAndUniquesDs = new DirectoryDsConfig("/data/temp/ddsmt/vvAndUniques/${hour.prev}", Some(CheckStrategies.timestamp))
    val createUniques = new RuleConfig("createUniques", paramDesc, prevVvAndUniquesDs, parquetAndPrevUniquesDs, new ExtCommandConfig(Iterable("/data/temp/writeToFile.sh", "copy " + prevVvAndUniquesDs + "/unique " + parquetAndPrevUniquesDs + "/unique")))

    val xodataDs = new GenericDsConfig("xodata")
    val loadVvToXodata = new RuleConfig("loadVvToXodata", paramDesc, vvAndUniquesDs, xodataDs, new ExtCommandConfig(Iterable("/data/temp/writeToFile.sh", "java -cp vidaview.db.transfer-0.0.4-SNAPSHOT.jar ru.imho.vidaview.etl.db.VidaView jdbc:postgresql://192.168.6.121:5432/vidaview vwuser hFciBhZQcJpyFyBR " + vvAndUniquesDs + "/report/ # Загрузка VV dataset в XODATA 1/час")))


    val storage = new StorageImpl("/data/temp/storage.jdbm")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        storage.close()
      }
    })

    val gs = new GraphService(params, Iterable(loadLogsFromAdFox, convertXmlToParquet, createVv, createUniques, loadVvToXodata), storage)
    gs.run()
  }
}
