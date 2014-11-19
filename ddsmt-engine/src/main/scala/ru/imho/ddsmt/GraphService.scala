package ru.imho.ddsmt

import scala.annotation.tailrec
import scalax.collection.mutable.Graph
import ru.imho.ddsmt.Base._
import scalax.collection.GraphEdge.DiEdge
import akka.actor.{Props, Actor, ActorSystem}
import scala.collection.mutable
import akka.routing.RoundRobinRouter

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
    val ruleToExec = graph.nodes.filter(n => n.value.isInstanceOf[Rule] && !isRuleExecuted(n, executedRules) && isRuleReadyToExecute(n, executedRules))
      .map(_.value.asInstanceOf[Rule])

    if (!ruleToExec.isEmpty) {
      ruleToExec.foreach(_.execute())
      executeRules(executedRules ++ ruleToExec)
    }
  }

  private def isRuleExecuted(rule: graph.NodeT, executedRules: scala.collection.Set[Rule]) = executedRules.contains(rule.value.asInstanceOf[Rule])

  private def isDsReady(ds: graph.NodeT, executedRules: scala.collection.Set[Rule]) = {
    ds.diPredecessors.isEmpty ||
      ds.diPredecessors.forall(rule => isRuleExecuted(rule, executedRules))
  }

  private def isRuleReadyToExecute(rule: graph.NodeT, executedRules: scala.collection.Set[Rule]) = {
    rule.diPredecessors.forall(ds => isDsReady(ds, executedRules))
  }

  def runInParallel(degree: Int) = {
    val system = ActorSystem("ddsmtSystem")
    val dispatcher = system.actorOf(Props(new Dispatcher(degree)), "dispatcher")
    dispatcher ! "start"
  }

  class Dispatcher(degree: Int) extends Actor {

    val workers = context.system.actorOf(Props[Worker].withRouter(RoundRobinRouter(nrOfInstances = degree)))

    val executedRules = mutable.Set[Rule]()
    val executingRules = mutable.Set[Rule]()
    val failedRules = mutable.Set[Rule]()
    val rulesNum = graph.nodes.count(n => n.value.isInstanceOf[Rule])

    def receive = {
      case result: Result => {
        if (result.success)
          executedRules += result.rule
        else
          failedRules += result.rule

        process()
      }
      case "start" => process()
    }

    private def process() {
      val ruleToExec = graph.nodes.filter(n => n.value.isInstanceOf[Rule] && !isRuleExecuting(n) && isRuleReadyToExecute(n, executedRules))
        .map(_.value.asInstanceOf[Rule])

      if (!ruleToExec.isEmpty) {
        ruleToExec.foreach(r => workers ! r)
        executingRules ++= ruleToExec
      } else {
        if (executedRules.size + failedRules.size == rulesNum) {
          Logger.info("The graph was fully processed. Rules stat: success - %d, error - %d".format(executedRules.size, failedRules.size))
          context.system.shutdown()
        } else if (executedRules.size + failedRules.size == executingRules.size) {
          Logger.error("The graph was not fully processed due to errors. Rules stat: success - %d, error - %d, not executed - %d".format(executedRules.size, failedRules.size, rulesNum - (executedRules.size + failedRules.size)))
          context.system.shutdown()
        }
      }
    }

    private def isRuleExecuting(rule: graph.NodeT) = executingRules.contains(rule.value.asInstanceOf[Rule])
  }

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

class Worker extends Actor {

  def receive = {
    case ruleToExec: Rule => {
      try {
        ruleToExec.execute()
        sender() ! Result(ruleToExec, true)
      } catch {
        case e: Throwable => sender() ! Result(ruleToExec, false)
      }
    }
  }
}

case class Result(rule: Rule, success: Boolean)
