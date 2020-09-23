package assignment2

import java.io.FileWriter
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.json4s.jackson.JsonMethods.{compact, parse}
import scala.collection.mutable

object task1 {
  def main(args: Array[String]):Unit={
    val case_number = args(0)
    val support = args(1).toInt
    val input_filepath = args(2)
    val output_filepath = args(3)
    
    val chunk_num = 2
    val p = 0.8
    val t = p * support

    val spark = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(spark)
    
    val start = System.currentTimeMillis()
    val rdd = sc.textFile(input_filepath).mapPartitionsWithIndex{
    (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    
    var basket: RDD[List[String]] = null
    if(case_number == "1"){
      basket = rdd.distinct().map(line=>(line.split(",")(0), line.split(",")(1))).partitionBy(new HashPartitioner(chunk_num)).groupByKey().map(_._2.toList).cache()
    }else if(case_number == "2"){
      basket = rdd.distinct().map(line=>(line.split(",")(1), line.split(",")(0))).partitionBy(new HashPartitioner(chunk_num)).groupByKey().map(_._2.toList).cache()
    }else{
      return
    }
    
    val phase_1 = basket.mapPartitions(chunk => a_prior(chunk, t/chunk_num)).reduceByKey((x,y)=>x.union(y)).sortByKey()
    var candidates = phase_1.collect()
    
    val phase_2 = basket.mapPartitions(chunk => count_candidates(chunk, candidates)).reduceByKey(_+_).sortBy(x => x._1.size).filter(x=> x._2>=support)
    var frequent_items = phase_2.collect()

    
    val writer = new FileWriter(output_filepath)
    
    writer.write("Candidates:\n")
    for ((pair_len, candidate) <- candidates){
      var str_list = new mutable.ListBuffer[String]
      for (c <- candidate){
        val c_str = c.mkString("\', \'")
        str_list += "(\'"+c_str+"\')"
      }
      str_list = str_list.sorted
      val line = str_list.mkString(",") + "\n\n"
      writer.write(line)
    }
    
    writer.write("Frequent Itemsets:\n")
    var prev_len = 1
    var str_list = new mutable.ListBuffer[String]
    for((item, count) <- frequent_items){
      val cur_len = item.size
      if(cur_len != prev_len){
        str_list = str_list.sorted
        val line = str_list.mkString(",")+ "\n\n"
        writer.write(line)
        prev_len = cur_len
        str_list = new mutable.ListBuffer[String]
      }
      val item_str = "(\'"+item.mkString("\', \'")+"\')"
      str_list += item_str
    }
    str_list = str_list.sorted
        val line = str_list.mkString(",")
        writer.write(line)
    
    writer.close()
    val duration = System.currentTimeMillis()-start
    println("Duration: "+duration/1000)
  }

  
  def a_prior(chunks: Iterator[List[String]], t:Double):Iterator[(Int, mutable.Set[Set[String]])]={
    var count = mutable.Map.empty[String, Int]
    var baskets = new mutable.ListBuffer[Set[String]]
    
    for (chunk <- chunks){
      baskets += chunk.toSet
      for (item <- chunk){
        if (!count.contains(item)){
          count += (item -> 1)
        }else{
          count(item) += 1
        }
      }
    }
     
    var candidates = mutable.Set.empty[Set[String]]
    var candidates_res = mutable.Map.empty[Int, mutable.Set[Set[String]]]
    for (c <- count){
      if (c._2 >= t) candidates += Set(c._1)
    }
    // candidates = mutable.Set.empty[Set[String]]++candidates.toList.sortWith((x,y)=>sort_set(x,y)).toSet
    candidates_res += (1 -> candidates)
    
    var pair_len = 2
    while (!candidates.isEmpty){
      var candidates_new = mutable.Set.empty[Set[String]]
      val seq = candidates.toSeq
      for (i <- 0 to candidates.size-2){
        for (j <- i+1 to candidates.size-1){
          var candidate_new = seq(i).union(seq(j)).toList.sorted.toSet
          if (candidate_new.size==pair_len) 
            candidates_new += candidate_new
        }
      }
      
      var count = mutable.Map.empty[Set[String], Int]
      for (candidate <- candidates_new){
        for (basket <- baskets){
          if (candidate.subsetOf(basket)){
            if (!count.contains(candidate)){
              count += (candidate -> 1)
            }else{
              count(candidate) += 1
            }
          }
        }
      }
      
      candidates = mutable.Set.empty[Set[String]]
      for (c <- count){
        if (c._2 >= t) candidates += c._1
      }
      
      if(!candidates.isEmpty){
        candidates_res += (pair_len -> candidates)
      }
      pair_len += 1    
    }
    
    return candidates_res.toIterator   
  }
  
  def count_candidates(basket:Iterator[List[String]], candidates_all:Array[(Int, mutable.Set[Set[String]])]):Iterator[(Set[String], Int)]={
    var count = mutable.Map.empty[Set[String], Int]
    for (chunk <- basket){
      for (candidates <- candidates_all){
        for (candidate <- candidates._2){
          if (candidate.subsetOf(chunk.toSet)){
            if (!count.contains(candidate)){
              count += (candidate -> 1)
            }else{
              count(candidate) += 1
            }
          }
        }
      }
    }
    return count.toIterator
  }
  
  
}