import java.io.FileWriter

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, parse}

object task3 {
  def main(args: Array[String]):Unit={
    val review_filepath = args(0)
    val business_filepath = args(1)
    val output_filepath_question_a = args(2)
    val output_filepath_question_b = args(3)

    val spark = new SparkConf().setAppName("task3").setMaster("local[*]")
    val sc = new SparkContext(spark)

    // default
    val review_RDD = sc.textFile(review_filepath).map{line=> val line_json = parse(line)
      (compact(line_json\"business_id"),compact(line_json\"stars").toFloat)
    }.partitionBy(new HashPartitioner(4))
    val business_RDD = sc.textFile(business_filepath).map{line=> val line_json = parse(line)
      (compact(line_json\"business_id"),compact(line_json\"city"))
    }.partitionBy(new HashPartitioner(4))
    val city_star_RDD = review_RDD.join(business_RDD).map(line=>(line._2._2, (line._2._1, 1)))

    // A. What is the average stars for each city?
    val city_star_ave = city_star_RDD.reduceByKey((x,y)=>(x._1+y._1, x._2+y._2)).mapValues(x=>x._1.toFloat/x._2.toFloat).cache()
    val city_star_ave_list_A = city_star_ave.sortBy(x=>(-x._2, x._1)).collect()

    // B. print top 10 cities with highest stars
    // Method1: Collect all the data, sort in python, and then print the first 10 cities
    val start_B1 = System.currentTimeMillis()
    val city_star_ave_list_B1 = city_star_ave.collect()
    city_star_ave_list_B1.sortBy(x=>(-x._2, x._1))
    for (city_star <- city_star_ave_list_B1.take(10))
    {
      println(city_star)
    }
    val m1 = System.currentTimeMillis()-start_B1

    // Method2: Sort in Spark, take the first 10 cities, and then print these 10 cities
    val start_B2 = System.currentTimeMillis()
    val city_star_ave_list_B2 = city_star_ave.takeOrdered(10)(Ordering[(Float, String)].on(x=>(-x._2, x._1)))
    for (city_star <- city_star_ave_list_B2)
    {
      println(city_star)
    }
    val m2 = System.currentTimeMillis()-start_B2

    val writer_a = new FileWriter(output_filepath_question_a)
    writer_a.write("city,stars\n")
    for (city_star <- city_star_ave_list_A)
    {
      writer_a.write(city_star._1.substring(1).dropRight(1)+","+city_star._2+"\n")
    }
    writer_a.close()

    val writer_b = new FileWriter(output_filepath_question_b)
    writer_b.write("{\n")
    writer_b.write("    \"m1\":  "+m1+",\n")
    writer_b.write("    \"m2\":  "+m2+",\n")
    writer_b.write("}")
    writer_b.close()
  }

}
