package org.vin.loggers.service;

import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.vin.datamall.dw.common.constant.Constants;

@org.springframework.stereotype.Service
public class Service {

	@Autowired
	KafkaTemplate kafkaTemplate;

	public void sendMessage(JSONObject logObject) {
		if (Constants.KAFKA_TOPIC_STARTUP.equals(logObject.get("type"))) {
			kafkaTemplate.send(Constants.KAFKA_TOPIC_STARTUP, logObject.toJSONString());
		} else {
			kafkaTemplate.send(Constants.KAFKA_TOPIC_EVENT, logObject.toJSONString());
		}
	}
}
