package ru.imho.ddsmt

import scala.annotation.tailrec
import scalax.collection.mutable.Graph
import ru.imho.ddsmt.Base._
import scalax.collection.GraphEdge.DiEdge

/**
 * Created by skotlov on 11/13/14.
 */
class GraphService(params: Map[String, Iterable[Param]], rules: Iterable[RuleConfig], storage: Storage) {

  val graph = buildGraph(params, rules)

  def run() = {
    executeRules(Set())
  }

  @tailrec
  private def executeRules(executedRules: Set[Rule]) {

    def isRuleExecuted(rule: graph.NodeT) = executedRules.contains(rule.value.asInstanceOf[Rule])

    def isDsReady(ds: graph.NodeT) = {
      ds.diPredecessors.isEmpty ||
        ds.diPredecessors.forall(rule => isRuleExecuted(rule))
    }

    def isRuleReadyToExecute(rule: graph.NodeT) = {
      rule.diPredecessors.forall(ds => isDsReady(ds))
    }

    val ruleToExec = graph.nodes.filter(n => n.value.isInstanceOf[Rule] && !isRuleExecuted(n) && isRuleReadyToExecute(n))
      .map(_.value.asInstanceOf[Rule])

    if (!ruleToExec.isEmpty) {
      ruleToExec.foreach(_.execute())
      executeRules(executedRules ++ ruleToExec)
    }
  }

  def runInParallel(degree: Int) = ???

  private def buildGraph(params: Map[String, Iterable[Param]], rules: Iterable[RuleConfig]): Graph[Node, DiEdge] = {
    val graph = Graph[Node, DiEdge]()

    def addRule(in: Iterable[DataSet], out: Iterable[DataSet], rule: Rule) {
      in.foreach(i => graph += DiEdge(i, rule))
      out.foreach(o => graph += DiEdge(rule, o))
    }

    rules.foreach(ruleConf => {
      if (ruleConf.param.isDefined && ruleConf.param.get.paramPolicy == ParamPolicy.forEach) {
        val paramName = ruleConf.param.get.paramName
        val exceptFor = ruleConf.param.get.exceptFor

        params(paramName)
          .filter(p => exceptFor.isEmpty || !params(exceptFor.get).exists(_ == p))
          .foreach(param => {
            val in = ruleConf.input.map(_.createDataSetInstance(param, paramName))
            val out = ruleConf.output.map(_.createDataSetInstance(param, paramName))
            val rule = Rule(ruleConf.name, in, out, ruleConf.cmd.createCommand(param, paramName))(storage)
            addRule(in, out, rule)
          })
      }
      else if (ruleConf.param.isEmpty) {
        val in = ruleConf.input.map(_.createDataSetInstance())
        val out = ruleConf.output.map(_.createDataSetInstance())
        val rule = Rule(ruleConf.name, in, out, ruleConf.cmd.createCommand())(storage)
        addRule(in, out, rule)
      }
      else {
        ???
      }
    })

    if (graph.isCyclic)
      throw new RuntimeException("graph is cyclic")
    graph
  }
}
