package ru.imho.ddsmt

import scala.xml.{Node, XML}
import ru.imho.ddsmt.params.Hour
import ru.imho.ddsmt.ds.{FtpDsConfig, DirectoryDsConfig, GenericDsConfig}
import ru.imho.ddsmt.commands.ExtCommandConfig
import ru.imho.ddsmt.Base.{CommandConfig, CheckStrategies, DataSetConfig, Param}

/**
 * Created by skotlov on 11/14/14.
 */
object ConfigParser {

  val paramFabric: Map[String, String => Iterable[Param]] = Map("Hour" -> (v => Hour(v)))

  val dataSetFabric: Map[String, Node => DataSetConfig] = Map(
    "generic" -> (n => new GenericDsConfig((n \ "@id").text)),
    "directory" -> (n => new DirectoryDsConfig((n \ "@path").text, checkStrategy(n))),
    "ftp" -> (n => new FtpDsConfig((n \ "@hostname").text, attrOpt(n, "username"), attrOpt(n, "password"), (n \ "@path").text, attrOpt(n, "fileNameRegex"), checkStrategy(n)))
  )

  val commandFabric: Map[String, Node => CommandConfig] = Map(
    "external" -> (n => new ExtCommandConfig((n \ "part").map(_.text)))
  )

  def parse(configFile: String): Config = {
    val config = XML.loadFile(configFile)

    val params = (config \ "params" \ "param").map(param => {
      (param \ "@name").text -> paramFabric((param \ "@type").text)((param \ "@value").text)
    })

    val rules = (config \ "rules" \ "rule").map(rule => {
      val name = (rule \ "@name").text
      val exceptFor = attrOpt(rule, "exceptFor")
      val param = Some(ParamDesc((rule \ "@paramName").text, ParamPolicy.withName((rule \ "@paramPolicy").text), exceptFor))

      val input = (rule \ "input" \ "_").map(i => dataSetFabric(i.label)(i))
      if (input.isEmpty) throw new RuntimeException("In Rule must be at least one Input")

      val output = (rule \ "output" \ "_").map(o => dataSetFabric(o.label)(o))
      if (output.isEmpty) throw new RuntimeException("In Rule must be at least one Output")

      val command = (rule \ "command" \ "_").map(c => commandFabric(c.label)(c))
      val cmd = if (command.size == 1) command.head else throw new RuntimeException("In Rule must be one Command")

      new RuleConfig(name, param, input, output, cmd)
    })

    new Config(params.toMap, rules)
  }

  private def attrOpt(n: Node, name: String): Option[String] =
    if ((n \ ("@" + name)).isEmpty) None else Some((n \ ("@" + name)).text)

  private def checkStrategy(n: Node) = if ((n \ "@checkStrategy").isEmpty) None else Some(CheckStrategies.withName((n \ "@checkStrategy").text))
}

class Config(val params: Map[String, Iterable[Param]], val rules: Iterable[RuleConfig])
