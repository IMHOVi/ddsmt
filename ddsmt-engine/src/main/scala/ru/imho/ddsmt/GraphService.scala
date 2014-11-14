package ru.imho.ddsmt

import ru.imho.ddsmt.params.{ApplyParamException, Param}
import scalax.collection.edge.LDiEdge
import scala.annotation.tailrec
import ru.imho.ddsmt.ds.DataSet
import scalax.collection.mutable.Graph
import scalax.collection.GraphEdge.EdgeLike

/**
 * Created by skotlov on 11/13/14.
 */
class GraphService(params: Map[String, Iterable[Param]], rules: Iterable[RuleConfig]) {

  val graph = buildGraph(params, rules)

  def run() = {
    val roots = graph.nodes.filter(_.diPredecessors.isEmpty)
    executeOutgoingEdgesOfCompletedNodes(roots.toSet, Set())
  }

  @tailrec
  private def executeOutgoingEdgesOfCompletedNodes(nodeToExec: Set[graph.NodeT], executedNodes: Set[graph.NodeT]) {
    val execNodes = nodeToExec.map(n => {
      n.outgoing.foreach(e => e.label.asInstanceOf[Rule].execute())
      n
    })
    val newExecutedNodes = executedNodes ++ execNodes

    val newNodeToExec = graph.nodes.filter(n => !newExecutedNodes.contains(n) && (n.diPredecessors.isEmpty || n.diPredecessors.forall(p => newExecutedNodes.contains(p))))
    if (!newNodeToExec.isEmpty) {
      executeOutgoingEdgesOfCompletedNodes(newNodeToExec.toSet, newExecutedNodes)
    }
  }

  def runInParallel(degree: Int) = ???

  private def buildGraph(params: Map[String, Iterable[Param]], rules: Iterable[RuleConfig]): Graph[DataSet, LDiEdge] = {
    val graph = Graph[DataSet, LDiEdge]()

    rules.foreach(rule => {
      if (rule.param.isDefined && rule.param.get.paramPolicy == ParamPolicy.forEach) {
        val paramName = rule.param.get.paramName
        params(paramName).foreach(param => {
          try {
            val in = rule.input.createDataSetInstance(param, paramName)
            val out = rule.output.createDataSetInstance(param, paramName)
            graph += LDiEdge(in, out)(Rule(in, out, rule.cmd.createCommand(param, paramName)))
          } catch {
            case e: ApplyParamException => {}// do nothing // todo use some better solution
          }
        })
      }
      else if (rule.param.isEmpty) {
        val in = rule.input.createDataSetInstance()
        val out = rule.output.createDataSetInstance()
        graph += LDiEdge(in, out)(Rule(in, out, rule.cmd.createCommand()))
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
