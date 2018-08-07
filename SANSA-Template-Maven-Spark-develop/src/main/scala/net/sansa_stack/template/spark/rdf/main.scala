package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import java.net.{ URI => JavaURI }
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.sql.{ SQLContext, Row, DataFrame }
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import net.sansa_stack.inference.utils.TripleUtils
import org.apache.spark.graphx.lib.ShortestPaths
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

object Test {

//  var pathList: ListBuffer[(Long, Long)] = new ListBuffer[(Long, Long)]
  //  var nodeList: ListBuffer[Long] = new ListBuffer[Long]
  var featureMap: HashMap[String, String] = new HashMap[String, String]
  var compTriples = List.empty[Triple[String, List[String], String]]
  var instanceTransactions : ListBuffer[HashSet[String]] = new ListBuffer[HashSet[String]]
  var itemTransaction : ListBuffer[HashSet[String]] = new ListBuffer[HashSet[String]]
  val query: String = "Patient,Visit,{Disease,Drug}"

  System.setProperty("hadoop.home.dir", "E:/Project/hadoop-common-2.2.0-bin-master/");
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"Triple reader example  $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|        Triple reader example       |")
    println("======================================")
    val triplesRDD = NTripleReader.load(spark, JavaURI.create(input))
    val tripleRDD = triplesRDD.map(triple => (triple.getSubject.toString, triple.getPredicate.toString, triple.getObject.toString))
    updateFeatures()
    println(featureMap)
    
    val tutleSubjectObject = tripleRDD.map { x => (x._1, x._3) }

    type VertexId = Long

    val indexVertexID = (tripleRDD.map(_._1) union tripleRDD.map(_._3)).distinct().zipWithIndex()
    val vertices: RDD[(VertexId, String)] = indexVertexID.map(f => (f._2, f._1))

    val tuples = tripleRDD.keyBy(_._1).join(indexVertexID).map(
      {
        case (k, (Triple(s, p, o), si)) => (o, (si, p))
      })

    val edges: RDD[Edge[String]] = tuples.join(indexVertexID).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    val graph = Graph(vertices, edges)

    graph.vertices.collect().foreach(println(_))

    //    val g = graph.connectedComponents.vertices.map(_.swap).groupByKey
    //    g.collect().foreach(println)

    println("edges")
    graph.edges.collect().foreach(println(_))

    val result = ShortestPaths.run(graph, Seq(3L))

    val shortestPath = result // result is a graph
      .vertices // we get the vertices RDD
      .filter({ case (vId, _) => vId == 5L }) // we filter to get only the shortest path from v1
      .first 

    createCompositionTriples(query, graph)
    
    println(instanceTransactions)
    //itemTransactions(tripleRDD)
    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Triple reader example") {

    head(" Triple reader example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triple format)")

    help("help").text("prints this usage text")
  }

  def getVertexId(graph: Graph[String, String], vertextName: String): Long = {
    val vert = graph.vertices.filter { case (_, name) => name.contains(vertextName) }
    //    println("ContextName", vertextName)
    //    vert.map(_._1).foreach(println)
    if (vert.map(_._1).isEmpty()) {
      return -1
    }
    return vert.map(_._1).first()
  }

  def getNodesInPath(graph: Graph[String, String], src: Long, dest: Long, pathList : ListBuffer[(Long, Long)]) :ListBuffer[(Long, Long)]= {
    if (src != -1 && dest != -1) {
      val res = graph.triplets.collect {
        case t if t.srcId == src => t.dstId
      }
      res.collect().map(obj => {
        if (obj != null) {
          pathList.append((src, obj))
          if (obj != dest)
            getNodesInPath(graph, obj, dest,pathList)
        }
      })
    }
    return pathList
  }

  def getRefinedPathList(pathList: ListBuffer[(Long, Long)], src: Long, dest: Long, nodeList: ListBuffer[Long]): ListBuffer[Long] = {
    nodeList.append(dest)
    val temp = pathList.filter { case (_, destId) => destId == dest }
    if (temp.isEmpty)
      return null
    val sourceNode = pathList.filter { case (_, destId) => destId == dest }(0)._1
    if (sourceNode != src) {
      nodeList.append(sourceNode)
      getRefinedPathList(pathList, src, sourceNode, nodeList)
    } else if (sourceNode == src) {
      nodeList.append(src)
    }
    return nodeList
  }

  def updateFeatures() {
    featureMap("PTN_XY21") = "Patient"
    featureMap("Male") = "Gender"
    featureMap("VISIT1") = "Visit"
    featureMap("VISIT2") = "Visit"
    featureMap("RHEX1") = "Rheumatology"
    featureMap("RHEX2") = "Rheumatology"
    featureMap("DIAG1") = "Diagnosis"
    featureMap("DIAG2") = "Diagnosis"
    featureMap("TREAT1") = "Treatment"
    featureMap("TREAT2") = "Treatment"
    featureMap("ULTRA1") = "Ultrasonography"
    featureMap("ULTRA2") = "Ultrasonography"
    featureMap("ULTRA3") = "Ultrasonography"
    featureMap("Malformation") = "Disease"
    featureMap("Bad Rotation") = "Disease"
    featureMap("Arthritis") = "Disease"
    featureMap("SystemicArthritis") = "Disease"
    featureMap("DT1") = "DrugTherapy"
    featureMap("DT2") = "DrugTherapy"
    featureMap("DT3") = "DrugTherapy"
    featureMap("Knee") = "Joint"
    featureMap("Wrist") = "Joint"
    featureMap("Rheumatology") = "Report"
    featureMap("Diagnosis") = "Report"
    featureMap("Treatment") = "Report"
    featureMap("Methotrexate") = "Drug"
    featureMap("Corticosteroids") = "Drug"
    featureMap("10") = "RHEX1_damageIndex"
    featureMap("15") = "RHEX2_damageIndex"
  }

  def getVertexName(graph: Graph[String, String], nodeList: ListBuffer[Long]): ListBuffer[String] = {
    var vertexNames: ListBuffer[String] = new ListBuffer[String]
    nodeList.foreach(nodeId => {
      val vert = graph.vertices.filter { case (id, _) => id == nodeId }
//      vert.foreach(println(_))
      if (!vert.map(_._2).isEmpty()) {
        val name = vert.map(_._2).first()
        if (name.contains("/"))
          vertexNames += name.substring(name.lastIndexOf("/") + 1)
        else
          vertexNames += name
      }
    })
    return vertexNames
  }

  def createCompositionTriples(queryPattern: String, graph: Graph[String, String]) {
    var pathList: ListBuffer[(Long, Long)] = new ListBuffer[(Long, Long)]
    var nodeList: ListBuffer[Long] = new ListBuffer[Long]
    var instanceTransactionList : HashSet[String] = new HashSet[String]
    var features = ListBuffer[String]()
    val targetConcept: String = queryPattern.substring(0, queryPattern.indexOf(","))
    val context: String = queryPattern.substring(queryPattern.indexOf(",") + 1, queryPattern.indexOf("{") - 1)
    if (queryPattern.substring(queryPattern.indexOf("{") + 1, queryPattern.indexOf("}")).contains(",")){
      val featList = queryPattern.substring(queryPattern.indexOf("{") + 1, queryPattern.indexOf("}")).split(",").toList
      println(featList)
      featList.foreach(feat => features += feat)
    }
    else
      features += queryPattern.substring(queryPattern.indexOf("{") + 1, queryPattern.indexOf("}"))

    val contextList = featureMap.filter { case (_, value) => value.trim.equalsIgnoreCase(context.trim) }
    val contextNodes = contextList.keySet
    //    println("List context", context)
    contextNodes.foreach(contextName => {
      features.foreach(feature => {
        val featureList = featureMap.filter { case (_, value) => value.equals(feature) }
        val featureNodes = featureList.keySet
        featureNodes.foreach(node => {
          pathList = getNodesInPath(graph, getVertexId(graph, contextName), getVertexId(graph, node),pathList)
          if (!pathList.isEmpty) {
            println()
//            println(pathList)
            nodeList = getRefinedPathList(pathList, getVertexId(graph, contextName), getVertexId(graph, node), nodeList)
            if (nodeList != null && !nodeList.isEmpty) {
//              println(nodeList.distinct.reverse)
              val path = getVertexName(graph, nodeList.distinct.reverse)
              if (path != null && path.length > 0) {
                val triple = (targetConcept, path.slice(0, path.length - 1), path(path.length - 1))
                println("triple")
                println(triple)
                compTriples :+ triple
                //Generating the Item Transactions from the Composition Triples - Start                 
                var itemTransaction = (featureMap(path(1)),featureMap(path(path.length - 1).replaceAll("^\"|\"$", "")),path(path.length - 1).replaceAll("^\"|\"$", ""))
                var MSC_Ic = itemTransaction._1
                var MSC_I = itemTransaction._2
                var i = itemTransaction._3
                println("{"+MSC_Ic+"."+MSC_I+"."+i+"}")
                instanceTransactionList += (path(path.length - 1))
                //Generating the Item Transactions from the Composition Triples - End             
              }
            }
          }
          nodeList = new ListBuffer[Long]
          pathList = new ListBuffer[(Long, Long)]
        })
      })
      instanceTransactions += instanceTransactionList
      instanceTransactionList = new HashSet[String]
    })
  }
 
  
//  def itemTransactions(tripleRDD: RDD[(String, String, String)]) = {
//    val context: String = query.substring(query.indexOf(",") + 1, query.indexOf("{") - 1)
//    var MSC_ic = ""
//    var MSC_i=""
//    compTriples.foreach(k=>{
//        MSC_i = featureMap(k._3)+"."+k._3
//        print("----------"+MSC_i)
//        
//        if(context == "Report"){
//          MSC_ic = featureMap(k._2(1))
//        }
//        else if(context == "Visit"){
//          MSC_ic = featureMap(k._2(0))
//        }
//        else{
//          print("Please enter the correct Context ")
//        }
//        println(MSC_ic+"."+MSC_i)
//    })
//
//  }
}