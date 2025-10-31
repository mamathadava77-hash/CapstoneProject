package com.capstone.receiver;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;

import com.capstone.domain.Account;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AccountDeserializer implements Deserializer<Account> {
	private ObjectMapper mapper = new ObjectMapper();

	@Override
	public Account deserialize(String topic, byte[] bytes) {
		Account account = null;
		try {
			account = mapper.readValue(bytes, Account.class);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return account;
	}

}
