package ru.imho.ddsmt

import scalax.collection.mutable.Graph
import ru.imho.ddsmt.Base._
import scalax.collection.GraphEdge.DiEdge
import akka.actor.{Props, Actor, ActorSystem}
import scala.collection.mutable
import akka.routing.RoundRobinRouter
import scala.concurrent.duration.Duration

/**
 * Created by skotlov on 11/13/14.
 */
class GraphService(params: Map[String, Iterable[Param]], rules: Iterable[RuleConfig], storage: Storage) {

  private val actorSystem = ActorSystem("ddsmtSystem")
  private val graph = buildGraph(params, rules)

  def run(concurrencyDegree: Int) = {
    val dispatcher = actorSystem.actorOf(Props(new Dispatcher(concurrencyDegree)), "dispatcher")
    dispatcher ! "start"
  }

  def toDot: String = {
    import scalax.collection.io.dot._
    val root = DotRootGraph(directed = true, id = Some("ddsmt_graph"))

    def getName(n: scalax.collection.Graph[Node, DiEdge]#NodeT) = n.value match {
      case r: Rule => r.name + "#" + r.hashCode()
      case ds: DataSet => ds.displayName
    }

    def edgeTransformer(innerEdge: scalax.collection.Graph[Node, DiEdge]#EdgeT): Option[(DotGraph,DotEdgeStmt)] = {
      val edge = innerEdge.edge
      Some(root, DotEdgeStmt(getName(edge.from), getName(edge.to), Nil))
    }

    actorSystem.shutdown()
    graph.toDot(root, edgeTransformer)
  }

  def awaitTermination(timeout: Duration): Unit = actorSystem.awaitTermination(timeout)

  def allDataSets = graph.nodes.filter(n => n.value.isInstanceOf[DataSet]).map(_.value.asInstanceOf[DataSet])

  def allRules = graph.nodes.filter(n => n.value.isInstanceOf[Rule]).map(_.value.asInstanceOf[Rule])

  class Dispatcher(concurrencyDegree: Int) extends Actor {

    val workers = context.system.actorOf(Props[Worker].withRouter(RoundRobinRouter(nrOfInstances = concurrencyDegree)))

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
      val ruleToExec = graph.nodes.filter(n => n.value.isInstanceOf[Rule] && !isRuleExecuting(n) && isRuleReadyToExecute(n))
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

    private def isRuleExecuted(rule: graph.NodeT) = executedRules.contains(rule.value.asInstanceOf[Rule])

    private def isDsReady(ds: graph.NodeT) = {
      ds.diPredecessors.isEmpty ||
        ds.diPredecessors.forall(rule => isRuleExecuted(rule))
    }

    private def isRuleReadyToExecute(rule: graph.NodeT) = {
      rule.diPredecessors.forall(ds => isDsReady(ds))
    }
  }

  private def buildGraph(params: Map[String, Iterable[Param]], rules: Iterable[RuleConfig]): Graph[Node, DiEdge] = {
    val graph = Graph[Node, DiEdge]()
    val scheduler = new Scheduler(actorSystem)

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
            val rule = Rule(ruleConf.name, in, out, ruleConf.cmd.createCommand(param, paramName))(storage, scheduler)
            addRule(in, out, rule)
          })
      }
      else if (ruleConf.param.isEmpty) {
        val in = ruleConf.input.map(_.createDataSetInstance())
        val out = ruleConf.output.map(_.createDataSetInstance())
        val rule = Rule(ruleConf.name, in, out, ruleConf.cmd.createCommand())(storage, scheduler)
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
