package ru.imho.ddsmt

import org.scalatest.FlatSpec
import ru.imho.ddsmt.Base._
import java.sql.Timestamp
import ru.imho.ddsmt.params.Hour
import org.scalamock.scalatest.MockFactory
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.collection.mutable

/**
 * Created by skotlov on 11/24/14.
 */
class GraphServiceTest extends FlatSpec with MockFactory {

  "GraphService" should "execute all of the commands in the correct order" in {
    val storage = stub[Storage]
    storage.getLastKnownChecksum _ when(*, *) returns Some(Const.checksumInStorage)

    val t1 = new TGraphService(storage, Set(), true)
    t1.run(1)
    t1.checkAllCommandsWasExecuted()
    t1.checkOrderOfExecution()

    val t2 = new TGraphService(storage, Set(), true)
    t2.run(16)
    t2.checkAllCommandsWasExecuted()
    t2.checkOrderOfExecution()
  }

  it should "not execute commands located after the point where the error occurred" in {
    import Const._
    val storage = stub[Storage]
    storage.getLastKnownChecksum _ when(*, *) returns Some(Const.checksumInStorage)

    val t2 = new TGraphService(storage, Set((sdToParquet, 1)), true)
    t2.run(16)
    t2.checkOrderOfExecution()

    t2.checkCommandWasExecuted(loadLogsFromAdFox, 0)
    t2.checkCommandWasExecuted(convertXmlToParquet, 0)
    t2.checkCommandWasExecuted(sdToParquet, 0)
    t2.checkCommandWasExecuted(createVvAndUnique, 0)
    t2.checkCommandWasExecuted(loadVvToXodata, 0)

    t2.checkCommandWasExecuted(loadLogsFromAdFox, 1)
    t2.checkCommandWasExecuted(convertXmlToParquet, 1)
    t2.checkCommandWasNotExecuted(sdToParquet, 1)
    t2.checkCommandWasNotExecuted(createVvAndUnique, 1)
    t2.checkCommandWasNotExecuted(loadVvToXodata, 1)

    t2.checkCommandWasExecuted(loadLogsFromAdFox, 2)
    t2.checkCommandWasExecuted(convertXmlToParquet, 2)
    t2.checkCommandWasExecuted(sdToParquet, 2)
    t2.checkCommandWasNotExecuted(createVvAndUnique, 2)
    t2.checkCommandWasNotExecuted(loadVvToXodata, 2)
  }

  it should "not execute any commands if data sources are not changed" in {
    val storage = stub[Storage]
    storage.getLastKnownChecksum _ when(*, *) returns Some(Const.checksumInStorage)

    val t1 = new TGraphService(storage, Set(), false)
    t1.run(1)
    t1.checkNoCommandsWasExecuted()
    t1.checkOrderOfExecution()

    val t2 = new TGraphService(storage, Set(), false)
    t2.run(16)
    t2.checkNoCommandsWasExecuted()
    t2.checkOrderOfExecution()
  }
}

object Const {
  val checksumInStorage = "checksum"

  val loadLogsFromAdFox = "loadLogsFromAdFox"
  val convertXmlToParquet = "convertXmlToParquet"
  val sdToParquet = "sdToParquet"
  val createVvAndUnique = "createVvAndUnique"
  val loadVvToXodata = "loadVvToXodata"
}

class TGraphService(storage: Storage, cmdToFail: Set[(String, Int)], isDsChanged: Boolean) {
  import Const._

  val readyDs = new {
    private val rds = mutable.Set[String]()

    def setReady(id: String) { rds += id }
    def isReady(id: String) = rds.contains(id)
  }

  val (gs, commands, dataSets) = buildGraph()

  def run(concurrencyDegree: Int) = {
    gs.run(concurrencyDegree)
    gs.awaitTermination(Duration(10, TimeUnit.SECONDS))
  }

  def checkOrderOfExecution() {
    assertExecutedBefore(commands((loadLogsFromAdFox, 0)), commands((convertXmlToParquet, 0)))
    assertExecutedBefore(commands((convertXmlToParquet, 0)), commands((createVvAndUnique, 0)))
    assertExecutedBefore(commands((sdToParquet, 0)), commands((createVvAndUnique, 0)))
    assertExecutedBefore(commands((createVvAndUnique, 0)), commands((loadVvToXodata, 0)))

    assertExecutedBefore(commands((createVvAndUnique, 0)), commands((createVvAndUnique, 1)))
    assertExecutedBefore(commands((loadLogsFromAdFox, 1)), commands((convertXmlToParquet, 1)))
    assertExecutedBefore(commands((convertXmlToParquet, 1)), commands((createVvAndUnique, 1)))
    assertExecutedBefore(commands((sdToParquet, 1)), commands((createVvAndUnique, 1)))
    assertExecutedBefore(commands((createVvAndUnique, 1)), commands((loadVvToXodata, 1)))

    assertExecutedBefore(commands((createVvAndUnique, 1)), commands((createVvAndUnique, 2)))
    assertExecutedBefore(commands((loadLogsFromAdFox, 2)), commands((convertXmlToParquet, 2)))
    assertExecutedBefore(commands((convertXmlToParquet, 2)), commands((createVvAndUnique, 2)))
    assertExecutedBefore(commands((sdToParquet, 2)), commands((createVvAndUnique, 2)))
    assertExecutedBefore(commands((createVvAndUnique, 2)), commands((loadVvToXodata, 2)))
  }

  def checkAllCommandsWasExecuted() {
    commands.foreach(c => assert(c._2.isExecuted))
  }

  def checkNoCommandsWasExecuted() {
    commands.foreach(c => assert(!c._2.isExecuted))
  }

  def checkCommandWasExecuted(name: String, hour: Int) {
    assert(commands((name, hour)).isExecuted)
  }

  def checkCommandWasNotExecuted(name: String, hour: Int) {
    assert(!commands((name, hour)).isExecuted)
  }

  private def assertExecutedBefore(before: TestCommand, after: TestCommand) {
    if (after.isExecuted)
      assert(before << after)
  }

  private def buildGraph() = {
    val params = Map(
      "everyHour" -> Hour("0 0 0-2 1 NOV ? 2014"),
      "everyHourFirst" -> Hour("0 0 0 1 NOV ? 2014"))

    def pd(name: String, except: Option[String]) = Some(ParamDesc(name, ParamPolicy.forEach, except))

    val afDs = new TestDsConfig("afDs/${everyHour}", true)
    val afLocalDs = new TestDsConfig("afLocalDs/${everyHour}")
    val sdDs = new TestDsConfig("sdDs/${everyHour}", true)
    val parqDs = new TestDsConfig("parqDs/${everyHour}")
    val vvDs = new TestDsConfig("vvDs/${everyHour}")
    val uniqDs = new TestDsConfig("uniqDs/${everyHour}")
    val prevUniqDs = new TestDsConfig("uniqDs/${everyHour.prev}")
    val xodataDs = new TestDsConfig("xodataDs")

    val cmdLoadLogsFromAdFox = new TestCommandConfig(loadLogsFromAdFox)
    val cmdConvertXmlToParquet = new TestCommandConfig(convertXmlToParquet)
    val cmdSdToParquet = new TestCommandConfig(sdToParquet)
    val cmdCreateVvAndUnique = new TestCommandConfig(createVvAndUnique)
    val cmdLoadVvToXodata = new TestCommandConfig(loadVvToXodata)

    val loadLogsFromAdFoxRule = new RuleConfig(loadLogsFromAdFox, pd("everyHour", None), Iterable(afDs), Iterable(afLocalDs), cmdLoadLogsFromAdFox)
    val convertXmlToParquetRule = new RuleConfig(convertXmlToParquet, pd("everyHour", None), Iterable(afLocalDs), Iterable(parqDs), cmdConvertXmlToParquet)
    val sdToParquetRule = new RuleConfig(sdToParquet, pd("everyHour", None), Iterable(sdDs), Iterable(parqDs), cmdSdToParquet)
    val createVvAndUniqueRule = new RuleConfig(createVvAndUnique, pd("everyHour", Some("everyHourFirst")), Iterable(parqDs, prevUniqDs), Iterable(vvDs, uniqDs), cmdCreateVvAndUnique)
    val loadVvToXodataRule = new RuleConfig(loadVvToXodata, pd("everyHour", None), Iterable(vvDs), Iterable(xodataDs), cmdLoadVvToXodata)
    val createVvAndUniqueFirstRule = new RuleConfig(createVvAndUnique, pd("everyHourFirst", None), Iterable(new TestDsConfig("parqDs/${everyHourFirst}")), Iterable(new TestDsConfig("vvDs/${everyHourFirst}"), new TestDsConfig("uniqDs/${everyHourFirst}")), cmdCreateVvAndUnique)

    val gs = new GraphService(params, Iterable(loadLogsFromAdFoxRule, convertXmlToParquetRule, sdToParquetRule, createVvAndUniqueRule, loadVvToXodataRule, createVvAndUniqueFirstRule), storage)

    val cmds = gs.allRules.map(_.cmd.asInstanceOf[TestCommand]).map(tc => ((tc.name, tc.hour), tc)).toMap
    assert(cmds.size == 15)

    val dss = gs.allDataSets.map(_.asInstanceOf[TestDs])
    assert(dss.size == 19)

    (gs, cmds, dss)
  }

  class TestDs(val id: String, val dataSetConfig: DataSetConfig) extends DataSet {

    override def checksum: Option[String] = Some(if (isDsChanged) checksumInStorage + "_2" else checksumInStorage)

    override def timestamp: Option[Timestamp] = None

    override def displayName: String = id
  }

  class TestDsConfig(id: String, isReady: Boolean) extends DataSetConfig {

    def this(id: String) = this(id, false)

    override def createDataSetInstance(): DataSet = throw new UnsupportedOperationException

    override def createDataSetInstance(param: Param, paramName: String): DataSet = {
      val ds =  new TestDs(param.applyToString(id, paramName), this)
      if (isReady)
        readyDs.setReady(ds.id)
      ds
    }

    override def checkStrategy: Option[CheckStrategies.Value] = Some(CheckStrategies.checksum)
  }

  case class TestCommand(name: String, hour: Int) extends Command {
    @volatile
    var startTime = 0L
    @volatile
    var endTime = 0L

    override def execute(rule: Rule): Unit = {
      startTime = System.currentTimeMillis()
      rule.input.foreach(i => assert(readyDs.isReady(i.id)))

      if (cmdToFail.contains((name, hour)))
        throw new RuntimeException("cmd fail")

      rule.output.foreach(o => readyDs.setReady(o.id))
      endTime = System.currentTimeMillis()
    }

    override def policy: CommandPolicy = new CommandPolicy(None, None)

    def <<(tc: TestCommand) = this.endTime <= tc.startTime

    def isExecuted = endTime != 0L
  }

  class TestCommandConfig(name: String) extends CommandConfig {

    override def createCommand(): Command = throw new UnsupportedOperationException

    override def createCommand(param: Param, paramName: String): Command = new TestCommand(name, param.asInstanceOf[Hour].hour)
  }
}
