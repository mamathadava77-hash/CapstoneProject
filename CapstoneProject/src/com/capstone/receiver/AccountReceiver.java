package com.capstone.receiver;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.capstone.domain.Account;

public class AccountReceiver {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AccountDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "account-group-1");
		// props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		KafkaConsumer<String, Account> consumer = new KafkaConsumer<>(props);
		List<String> topics = Collections.singletonList("accountopen-topic");
		consumer.subscribe(topics);
		while (true) {
			ConsumerRecords<String, Account> records = consumer.poll(Duration.ofSeconds(20));
			records.forEach(record -> {
				System.out.println("Key: " + record.key() + ", Partition: " + record.partition());
				System.out.println("value:");
				Account account = record.value();
				System.out.println("accNo:" + account.getAccountNumber() + "\tcustName:" + account.getCustomerId()
						+ "\taccType:" + account.getAccountType() + "\tbranch:" + account.getBranch());
			});
		}
	}
}
