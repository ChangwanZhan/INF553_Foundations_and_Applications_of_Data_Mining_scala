package assignment4
import org.apache.spark.{SparkContext, SparkConf, sql}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.io.{File, PrintWriter}
import org.graphframes._



object task1 {
	def main(args: Array[String]): Unit = {
		val input_file = args(0)
		val output_file = args(1)
		val spark = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(spark)
		sc.setLogLevel("ERROR")
	  val rdd = sc.textFile(input_file).map(x => x.split(" "))
	  
	  val edges_set = rdd.map(x=>(x(0), x(1))).collect()
	  val edges_set_undirected = mutable.Set[(String, String)]()
	  val vertices_set = mutable.Set[String]()
	  
	  for(edge <- edges_set){
	    vertices_set.add(edge._1)
	    vertices_set.add(edge._2)
	    if(!edges_set_undirected.contains(edge))
	      edges_set_undirected.add(edge)
	    val edge2 = (edge._2, edge._1)
	    if(!edges_set_undirected.contains(edge2))
	      edges_set_undirected.add(edge2)
	  }
	
		val edges_list_undirected = edges_set_undirected.toList
		val vertices_list = vertices_set.toList
		val sqlContext = new sql.SQLContext(sc)
		val vertices = sqlContext.createDataFrame(vertices_list.map(Tuple1(_))).toDF("id")
		val edges = sqlContext.createDataFrame(edges_list_undirected.map(x =>(x._1, x._2))).toDF("src", "dst")
		
		val g = GraphFrame(vertices, edges)
		val result = g.labelPropagation.maxIter(5).run()
		
		val community_list = result.select("id", "label").collect()
		val communities = mutable.Map[String, ListBuffer[String]]()
		for (community <- community_list){
		  val id = community(0).toString()
		  val label = community(1).toString()
		  if(!communities.contains(label)){
		    communities(label) = ListBuffer[String]()
		  }
		  communities(label).append(id)
		}
		
		val communities_sorted = mutable.Map[String, ListBuffer[String]]()
		for ((label, ids) <- communities){
		  communities_sorted(label) = ids.sorted
		}
		val communities_res = communities_sorted.toSeq.sortWith((a, b) => a._2.size < b._2.size || (a._2.size == b._2.size && a._2(0) < b._2(0)))
		
		var pw = new PrintWriter(new File(output_file))
		for ((_,community) <- communities_res)
		  pw.write("'"+community.mkString("', '")+"'\n")
		pw.close()
	}

}
