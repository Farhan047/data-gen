import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import scala.io.StdIn

object WikiMedia {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder().appName("WikiMedia").master("local").getOrCreate()

  import spark.implicits._

  println("Spark UI @ :", spark.sparkContext.uiWebUrl)

  def readAsDataset(): Unit = {
    println("Hello Farhan!!")
    //    /Users/farhan/work/data/wikimedia-rawfiles
    //    /Users/farhan/Downloads/pageviews-20201110-120000.gz
    val inputPath = "/Users/farhan/work/data/wikimedia-rawfiles"

    val df = spark
      .read
      .text(inputPath)
    //      .limit(10000)


    val pageViewsDF = df
      .map(x => {
        val valuesInArray = x.mkString("").split(" ")
        val domain_code = valuesInArray(0)
        val page_title = domain_code + "|" + valuesInArray(1)
        val count_views = valuesInArray(2).toLong
        val total_response_size = valuesInArray(3)

        (domain_code, page_title, count_views, total_response_size)
      })
      .toDF("domain_code", "page_title", "count_views", "total_response_size")
//      .persist()

    println("pageViewsDF.count() : " + pageViewsDF.count())


    pageViewsDF
      .select("page_title", "count_views")
      .groupBy("page_title")
      .sum("count_views")
      .sort(col("sum(count_views)").desc)
      .show(10, false)


    //    val allDomains = pageViewsDF.select("domain_code").distinct().collect()
    //    println(s"Total ${allDomains.size} domains")
    //    allDomains.map(_.mkString("")).foreach(println)

    pageViewsDF
      .select("domain_code", "count_views")
      .groupBy("domain_code")
      .sum("count_views")
      .sort(col("sum(count_views)").desc)
      .show(10, false)

  }

  def readAsRDD()={
    println("Hello Farhan!!")
    //    /Users/farhan/work/data/wikimedia-rawfiles
    //    /Users/farhan/Downloads/pageviews-20201110-120000.gz
    val inputPath = "/Users/farhan/work/data/wikimedia-rawfiles"

    val rdd = spark
      .sparkContext
      .textFile(inputPath)
    //      .limit(10000)


    val pageViewsDF = rdd
      .map(x => {
        val valuesInArray = x.mkString("").split(" ")
        val domain_code = valuesInArray(0)
        val page_title = domain_code + "|" + valuesInArray(1)
        val count_views = valuesInArray(2).toLong
        val total_response_size = valuesInArray(3)

        (domain_code, page_title, count_views, total_response_size)
      })
      .toDF("domain_code", "page_title", "count_views", "total_response_size")
//      .persist()

    println("pageViewsDF.count() : " + pageViewsDF.count())


    pageViewsDF
      .select("page_title", "count_views")
      .groupBy("page_title")
      .sum("count_views")
      .sort(col("sum(count_views)").desc)
      .show(10, false)


    //    val allDomains = pageViewsDF.select("domain_code").distinct().collect()
    //    println(s"Total ${allDomains.size} domains")
    //    allDomains.map(_.mkString("")).foreach(println)

    pageViewsDF
      .select("domain_code", "count_views")
      .groupBy("domain_code")
      .sum("count_views")
      .sort(col("sum(count_views)").desc)
      .show(10, false)

  }

  def main(args: Array[String]): Unit = {
//    timeIt {
//      readAsDataset()
//    }

    timeIt {
      readAsRDD()
    }

    println("Awaiting input to proceed ...")
    val str = StdIn.readLine()
    println(s"Entered '$str' Ending program")
  }

  def timeIt[R](block: => R): R = {
    val start = System.nanoTime()
    val result = block // call-by-name
    val end = System.nanoTime()
    val totalSeconds = (end - start) / 1000000000.0
    println("Elapsed time: " + totalSeconds + " secs")
    result
  }

}
