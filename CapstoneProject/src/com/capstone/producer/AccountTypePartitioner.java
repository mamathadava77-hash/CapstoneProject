package com.capstone.producer;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class AccountTypePartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object val, byte[] valBytes, Cluster cluster) {
		String type = (String) key;
		if ("CA".equalsIgnoreCase(type))
			return 0;
		if ("SB".equalsIgnoreCase(type))
			return 1;
		if ("RD".equalsIgnoreCase(type))
			return 2;
		if ("LOAN".equalsIgnoreCase(type))
			return 3;
		return 0;
	}

	@Override
	public void close() {

	}

}
