package assignment1

import java.io.FileWriter
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, parse}

object task1 {
  def main(args: Array[String]):Unit={
    val review_filepath = args(0)
    val output_filepath = args(1)

    val spark = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(spark)

    val review_RDD = sc.textFile(review_filepath).map{line=> val line_json = parse(line)
      (compact(line_json\"user_id"), compact(line_json\"date").split(" ",2)(0).split("-",3)(0), compact(line_json\"business_id"))
    }.coalesce(16).cache()

    // A. the total number of reviews
    val n_review = review_RDD.count()
    print(n_review)

    // B. the number of reviews in 2018
    val n_review_2018 = review_RDD.filter(line=>line._2== "2018").count()

    // C. the number of distinct users who wrote reviews
    val user_map = review_RDD.map(line=>(line._1,1)).reduceByKey(_+_).cache()
    val n_user = user_map.count()

    // D. the top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
    val top10_user = user_map.takeOrdered(10)(Ordering[(Int, String)].on(x=>(-x._2, x._1))).mkString(",").replaceAll("\\(","[").replaceAll(",","\\,").replaceAll("\\)","]")

    // E. the number of distinct business that have been reviewed
    val business_map = review_RDD.map(line=>(line._3,1)).reduceByKey(_+_).cache()
    val n_business = business_map.count()

    // F. the top 10 businesses that had the largest numbers of reviews and the number of reviews they had
    val top10_business = business_map.takeOrdered(10)(Ordering[(Int, String)].on(x=>(-x._2, x._1))).mkString(",").replaceAll("\\(","[").replaceAll(",","\\,").replaceAll("\\)","]")

    val writer = new FileWriter(output_filepath)
    writer.write("{\n")
    writer.write("    \"n_review\":  "+n_review+",\n")
    writer.write("    \"n_review_2018\":  "+n_review_2018+",\n")
    writer.write("    \"n_user\":  "+n_user+",\n")
    writer.write("    \"top10_user\":  ["+top10_user+"],\n")
    writer.write("    \"n_business\":  "+n_business+",\n")
    writer.write("    \"top10_business\":  ["+top10_business+"],\n")
    writer.write("}")

    writer.close()
  }
}