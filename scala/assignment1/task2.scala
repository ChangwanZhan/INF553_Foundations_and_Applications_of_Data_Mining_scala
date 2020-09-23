import java.io.FileWriter

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, parse}

object task2 {
  def main(args: Array[String]):Unit={
    val review_filepath = args(0)
    val output_filepath = args(1)
    val n_partition = args(2).toInt

    val spark = new SparkConf().setAppName("task2").setMaster("local[*]")
    val sc = new SparkContext(spark)

    // default
    val reviews_RDD = sc.textFile(review_filepath).map{line=> val line_json = parse(line)
      (compact(line_json\"business_id"),1)
    }.cache()
    // val reviews_RDD = sc.textFile(review_filepath).map(line=>parse(line+"")).map(line=>((line\"business_id").extract[String],1)).cache()
    val default_reviews_RDD = reviews_RDD.repartition(16).cache()
    val default_n_items = "["+default_reviews_RDD.glom().map(_.length).collect().mkString(",")+"]"
    val start_default = System.currentTimeMillis()
    default_reviews_RDD.reduceByKey(_+_).takeOrdered(10)(Ordering[(Int, String)].on(x=>(-x._2, x._1)))
    val default_exe_time = System.currentTimeMillis() - start_default

    // customized
    val customized_reviews_RDD = reviews_RDD.partitionBy(new HashPartitioner(n_partition)).cache()
    val customized_n_items = "["+customized_reviews_RDD.glom().map(_.length).collect().mkString(",")+"]"
    val start_customized = System.currentTimeMillis()
    customized_reviews_RDD.reduceByKey(_+_).takeOrdered(10)(Ordering[(Int, String)].on(x=>(-x._2, x._1)))
    val customized_exe_time = System.currentTimeMillis() - start_customized

    val writer = new FileWriter(output_filepath)
    writer.write("{\n")
    writer.write("    \"default\":{\n")
    writer.write("        \"n_partition\":  16,\n")
    writer.write("        \"n_items\":  "+default_n_items+",\n")
    writer.write("        \"exe_time\":  "+default_exe_time+"\n")
    writer.write("    },\n")
    writer.write("    \"customized\":{\n")
    writer.write("        \"n_partition\":  "+n_partition+",\n")
    writer.write("        \"n_items\":  "+customized_n_items+",\n")
    writer.write("        \"exe_time\":  "+customized_exe_time+"\n")
    writer.write("    }\n")
    writer.write(" }")

    writer.close()
  }

}
