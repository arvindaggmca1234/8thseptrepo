package main.scala
import org.apache.spark.sql.SparkSession
object Test1 {

    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder
        .appName("Word Count")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
      val textFile = spark.sparkContext.textFile("src/main/resources/test.txt")
      val counts = textFile.flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_+_)
      counts.collect()
       counts.saveAsTextFile("hdfs://user/text/test1/test2")
    }

}
