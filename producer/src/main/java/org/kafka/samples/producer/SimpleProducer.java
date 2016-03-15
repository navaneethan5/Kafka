package org.kafka.samples.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {

	private Properties kafkaProps = new Properties();
	private String topic;
	private ProducerConfig config;
	private Producer<String, String> producer;

	public static void main(String[] args) {

		SimpleProducer counter = new SimpleProducer();

		String brokerList = args[0];
		counter.topic = args[1];
		String sync = args[2];
		int delay = Integer.valueOf(args[3]);
		int count = Integer.valueOf(args[4]);

		counter.configure(brokerList, sync);
		counter.start();

		long startTime = System.currentTimeMillis();
		System.out.println("System is starting...");
		counter.produce("Started sending messages");

		for (int i = 0; i < count; i++) {
			counter.produce(Integer.toString(i));
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		long endTime = System.currentTimeMillis();
		System.out.println("We are done... This task took  "
				+ (endTime - startTime) + "ms");
		counter.produce("We are done... This task took  "
				+ (endTime - startTime) + "ms");

		counter.producer.close();
		System.exit(0);

	}

	private void configure(String brokerList, String sync) {

		kafkaProps.put("metadata.broker.list", brokerList);
		kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
		// kafkaProps.put("request.required.acks", 1);
		kafkaProps.put("producer.type", sync);

		config = new ProducerConfig(kafkaProps);
	}

	private void start() {
		producer = new Producer<String, String>(config);
	}

	private void produce(String s) {
		KeyedMessage<String, String> message = new KeyedMessage<String, String>(
				topic, null, s);
		producer.send(message);
	}

}
