package demo.spark

import java.io._
import java.lang._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TestConnectSpark {
	def main(args: Array[String]): Unit = {
		val sc = new SparkContext("local", "Test Spark Application")
		val rdd = sc.textFile("test_log").cache()
		var counts = rdd.map(x => x.split(" ")(8)).map(x => (x,1)).reduceByKey(a,b => a+b).sortByKey(true)
		var output = counts.collect()  
		for ((word, count) <- output)
			printf("%s: %i", word, count)

		sc.stop()	
	}
}