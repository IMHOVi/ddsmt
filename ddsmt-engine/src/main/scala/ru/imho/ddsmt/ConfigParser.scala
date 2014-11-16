package ru.imho.ddsmt

import scala.xml.{Node, XML}
import ru.imho.ddsmt.params.Hour
import ru.imho.ddsmt.ds.{DirectoryDsConfig, GenericDsConfig}
import ru.imho.ddsmt.commands.ExtCommandConfig
import ru.imho.ddsmt.Base.{CommandConfig, CheckStrategies, DataSetConfig, Param}

/**
 * Created by skotlov on 11/14/14.
 */
object ConfigParser {

  val paramFabric: Map[String, String => Iterable[Param]] = Map("Hour" -> (v => Hour(v)))

  val dataSetFabric: Map[String, Node => DataSetConfig] = Map(
    "generic" -> (n => new GenericDsConfig((n \ "@id").text)),
    "directory" -> (n => new DirectoryDsConfig((n \ "@path").text, if ((n \ "@checkStrategy").isEmpty) None else Some(CheckStrategies.withName((n \ "@checkStrategy").text))))
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
      val param = Some(ParamDesc((rule \ "@paramName").text, ParamPolicy.withName((rule \ "@paramPolicy").text)))

      val input = (rule \ "input" \ "_").map(i => dataSetFabric(i.label)(i))
      if (input.isEmpty) throw new RuntimeException("In Rule must be at least one Input")

      val output = (rule \ "output" \ "_").map(o => dataSetFabric(o.label)(o))
      if (output.isEmpty) throw new RuntimeException("In Rule must be at least one Output")

      val command = (rule \ "command" \ "_").map(c => commandFabric(c.label)(c))
      val cmd = if (command.size == 1) command.head else throw new RuntimeException("In Rule must be only one Command")

      new RuleConfig(name, param, input, output, cmd)
    })

    new Config(params.toMap, rules)
  }
}

class Config(val params: Map[String, Iterable[Param]], val rules: Iterable[RuleConfig])
