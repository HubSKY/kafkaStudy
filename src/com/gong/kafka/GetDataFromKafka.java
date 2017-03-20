package com.gong.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class GetDataFromKafka implements Runnable {
	private String topic;

	public GetDataFromKafka(String topic) {
		this.topic = topic;
	}

	public static void main(String[] args) {
		GetDataFromKafka gdkast = new GetDataFromKafka("test");
		new Thread(gdkast).start();
	}

	public void run() {
		System.out.println("start runing consumer");

		Properties properties = new Properties();
		properties.put("zookeeper.connect", "172.20.104.92:2181");// ����zk
		properties.put("group.id", "CMMtest");// ����Ҫʹ�ñ�������ƣ� //
												// ��������ߺ������߶���ͬһ�飬���ܷ���ͬһ���ڵ�topic����
		properties.put("auto.offset.reset", "largest");
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); // һ�δ������л�ȡһ������
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// ��ȡÿ�ν��յ����������
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		while (iterator.hasNext()) {
			String message = new String(iterator.next().message());
			consumer.commitOffsets();
			System.out.println(message);
		}
	}

}
