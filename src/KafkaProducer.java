package demo.kafka;

import java.util.Properties;
import java.util.Date;
import java.util.Random;
import java.io.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/*
 * Producer Demo
 *
 */
public class KafkaProducer 
{
	private final Producer<String, String> producer;
	public final static String TOPIC = "test_log";


	public KafkaProducer(){
		Properties props = new Properties();
		//此处配置的是kafka的端口
		props.put("metadata.broker.list", "192.168.116.37:9092");

		//配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");

		//request.required.ackss
		props.put("request.required.acks","-1");

		producer = new Producer<String, String>(new ProducerConfig(props));
	}

	public void Produce(String temp) {
		producer.send(new KeyedMessage<String, String>(TOPIC, temp));
	}

	public static void main (String[] args) throws IOException,InterruptedException{
		KafkaProducer producer = new KafkaProducer();
		File file = new File("test_log");
		InputStream is = new InputStream(file);
		DataInputStream dis = new DataInputStream(is);
		for (int i = 0; i < 10000; i++) {
			String temp = dis.readLine();
			producer.Produce(temp);
			Thread.sleep(1000); 
		}
	}
}