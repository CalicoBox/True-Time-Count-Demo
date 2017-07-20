package demo.spark.stream

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

object App {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Test Kafka to Spark Stream Application")
		val ssc = new StreamingContext(conf, Seconds(1))
		val zkQuorum = "192.168.116.37:2181"
		val group = "test_group"
		val topicpMap = "log_test".map((_,1)).toMap
		val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap)

		lines.print()
		
		ssc.start()
		ssc.awaitTermination()
	}
}