package com.capstone.producer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.capstone.domain.Account;
import com.capstone.serializer.AccountSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaAccountSenderApp {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AccountSerializer.class);
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AccountTypePartitioner.class);
		KafkaProducer<String, Account> producer = new KafkaProducer<>(props);
		String topic = "accountopen-topic";

		for (int i = 1001; i <= 1010; i++) {
			int cust = 23;
			Account account = new Account(i + cust, i, "CA", "Hebbal");
			ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "CA", account);
			producer.send(record);
		}
		for (int i = 2001; i <= 2010; i++) {
			int cust = 56;
			Account account = new Account(i + cust, i, "SB", "Ulsoor");
			ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "SB", account);
			producer.send(record);
		}

		for (int i = 3001; i <= 3010; i++) {
			int cust = 67;
			Account account = new Account(i + cust, i, "RD", "Hebbal");
			ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "RD", account);
			producer.send(record);
		}
		for (int i = 4001; i <= 4010; i++) {
			int cust = 78;
			Account account = new Account(i + cust, i, "Loan", "Panjagutta");
			ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "Loan", account);
			producer.send(record);
		}
		producer.close();
		System.out.println("Messages sent");

	}
}
