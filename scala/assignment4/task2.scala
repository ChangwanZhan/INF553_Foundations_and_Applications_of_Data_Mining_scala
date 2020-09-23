package assignment4

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.io.{File, PrintWriter}
import scala.math.{max, min}


object task2 {
	def main(args: Array[String]): Unit = {
		val input_file = args(0)
		val betweenness_output_file = args(1)
		val community_output_file = args(2)
		val spark = new SparkConf().setAppName("task2").setMaster("local[*]")
    val sc = new SparkContext(spark)
		sc.setLogLevel("ERROR")
	  val rdd = sc.textFile(input_file).map(x => x.split(" ")).map(x => (min(x(0).toInt, x(1).toInt).toString(), max(x(0).toInt, x(1).toInt).toString())).cache()
		val edges_list = rdd.collect()
		val graph = mutable.Map[String, mutable.Set[String]]()
		for (edge <- edges_list) {
			if (!graph.contains(edge._1))
				graph += (edge._1 -> mutable.Set[String]())
			if (!graph.contains(edge._2))
				graph += (edge._2 -> mutable.Set[String]())
			graph(edge._1).add(edge._2)
			graph(edge._2).add(edge._1)
		}
		val vertices = mutable.Set[String]()
		for (i <- rdd.map(x => x._1).collect())
			vertices += i
		for (j <- rdd.map(x => x._2).collect())
			vertices += j
			
		//Betweenness		
		val betweenness = gen_betweenness(vertices, graph)
		val betweenness_list = betweenness.toList.map(x => (x._1._1, x._1._2, x._2)).sortBy(x => (-x._3, x._1, x._2))
		var pw = new PrintWriter(new File(betweenness_output_file))
		for (b <- betweenness_list)
			pw.write("('" + b._1 + "', '" + b._2 + "'), " + b._3 + "\n")
		pw.close()

		// Community Detection
		var communities = gen_communities(vertices, graph, betweenness)
		communities = communities.sortBy(x => (x.length, x.head))
		pw = new PrintWriter(new File(community_output_file))
		for (c <- communities)
			for (i <- c) {
				pw.write("'" + i + "'")
				if (c.indexOf(i) == c.length - 1)
					pw.write("\n")
				else
					pw.write(",")
			}
		pw.close()
	}
	
	def gen_betweenness(vertices: mutable.Set[String], graph: mutable.Map[String, mutable.Set[String]]): mutable.Map[(String, String), Double] = {
		var betweenness = mutable.Map[(String, String), Double]()
		for (vertex <- vertices) {
			val (bfs_tree, sequence) = bfs(vertex, graph)
			var path_count = mutable.Map[String, Double]()
			for (seq <- sequence){
			  val child = seq
			  val parent = bfs_tree(child)
			  if (parent.isEmpty){
			    path_count(child) = 1.0
			  }else{
			    path_count(child) = 0.0
			    for(p <- parent){
			      path_count(child) = path_count(child)+path_count(p)
			    }
			  }
			}
			var vertices_sum = mutable.Map[String, Double]()
			for (seq <- sequence.reverse){
			  val d = seq
			  val s = bfs_tree(d)
			  if (!s.isEmpty){
			    if (!vertices_sum.contains(d)){
			        vertices_sum(d) = 1.0
			    }
			    for (p <- s){
			      if (!vertices_sum.contains(p)){
			        vertices_sum(p) = 1.0
			       }    
			      vertices_sum(p) = vertices_sum(p) + vertices_sum(d) * path_count(p) / path_count(d) 
			      val edge = List(p,d).sortWith(_<_)
			      val edge_t = edge match {
              case List(a, b) => (a, b)
            }
			      if (!betweenness.contains(edge_t)){
			        betweenness(edge_t) = 0
			      }
			      betweenness(edge_t) = betweenness(edge_t) + vertices_sum(d) * path_count(p) / path_count(d) / 2
			    }
			  }
			  
			}
		}
		return betweenness
	}
		
	def gen_communities(vertices: mutable.Set[String], graph: mutable.Map[String, mutable.Set[String]], betweenness: mutable.Map[(String, String), Double]): ListBuffer[ListBuffer[String]] = {
	  val m = betweenness.size
	  var tmp_betweenness = mutable.Map[(String, String), Double]() ++ betweenness
	  val tmp_graph = mutable.Map[String, mutable.Set[String]]() ++ graph
	  var max_modularity = -1.0
	  var res = ListBuffer[ListBuffer[String]]()
	  while (!tmp_betweenness.isEmpty){
	    val communities = divide_communities(tmp_graph, vertices)
	    var modularity = 0.0
	    for (community <- communities){
	      for (i <- community){
	        for (j <- community) {
					val k_i = graph(i).size
					val k_j = graph(j).size
					var a_ij = 0.0
					if (graph(i).contains(j))
						a_ij = 1
					modularity += a_ij - k_i * k_j / (2 * m.toFloat)
				  }
	      }
	    }
			modularity = modularity / (2 * m.toDouble)
			if (modularity > max_modularity) {
				max_modularity = modularity
				res = communities
			}
			val max_betweenness: Double = tmp_betweenness.values.max
			var removed_edges = ListBuffer[(String, String)]()
			for (i <- tmp_betweenness)
				if (i._2 == max_betweenness)
					removed_edges += i._1
			for (edge <- removed_edges) {
				tmp_graph(edge._1) = tmp_graph(edge._1).filter(_ != edge._2)
				tmp_graph(edge._2) = tmp_graph(edge._2).filter(_ != edge._1)
			}
			tmp_betweenness = gen_betweenness(vertices, tmp_graph)
	  }
	  return res
	}
	
	
	def bfs(root: String, graph: mutable.Map[String, mutable.Set[String]]): (mutable.Map[String, mutable.Set[String]], ListBuffer[String]) = {
		val queue = ListBuffer[(String, Int)]()
		queue.append((root,1))
		val sequence = ListBuffer[String]()
		val res = mutable.Map[String, mutable.Set[String]]()
		res(root) = mutable.Set[String]()
		var level_cur = 0
		val shortestPath = mutable.Map[String, Int]()
		var visited = mutable.Set[String]()
		visited.add(root)
		var visited_cur = mutable.Set[String]()
		var visited_all = mutable.Set[String]()
		for(q <- queue){
		  if (!visited_all.contains(q._1)){
		    sequence.append(q._1)
		  }
		  visited_all.add(q._1)
		  val d = q._1
		  val level = q._2
		  if (level != level_cur){
		    level_cur = level
		    visited = visited.union(visited_cur)
		    visited_cur = mutable.Set[String]()
		  }
		  for(v <- graph(d)){
		    if(!visited.contains(v)){
		      visited_cur.add(v)
		      queue.append((v, level+1))
		      if(!res.contains(v)){
		        res(v) = mutable.Set[String]()
		      }
		      res(v).add(d)
		    }
		  }
		}
		return (res, sequence)
	}
	
		
	def divide_communities(graph: mutable.Map[String, mutable.Set[String]], vertices: mutable.Set[String]): ListBuffer[ListBuffer[String]] = {
		var tmp_vertices = mutable.Set[String]()
		for (vertex <- vertices)
			tmp_vertices += vertex
		val communities = ListBuffer[ListBuffer[String]]()
		while (tmp_vertices.nonEmpty) {
	  	val vertex = tmp_vertices.toList.head
      val community = mutable.Set[String]()
  		val queue = ListBuffer[String]()
  		queue.append(vertex)
  		while (queue.nonEmpty) {
  			val s = queue.remove(0)
  			community.add(s)
  			for (d <- graph(s)){
  			  if (!community.contains(d))
  					queue.append(d)
  			}	
  		}
	  	communities.append(community.toList.sorted.to[ListBuffer])
			tmp_vertices = tmp_vertices.diff(community)

		}
		return communities
	}

}
