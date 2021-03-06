package org.vin.loggers.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.vin.loggers.service.Service;

import java.util.Calendar;


@RestController
public class LoggerController {

	@Autowired
	Service service;

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class);

	@PostMapping("log")
	public String log(@RequestParam("log") String logJson) {
		System.out.println(logJson);

		JSONObject jsonObject = JSON.parseObject(logJson);

		Calendar today = Calendar.getInstance();

//		yesterday.add(Calendar.DATE ,-1);

		jsonObject.put("ts", today.getTimeInMillis());

		service.sendMessage(jsonObject);

		logger.info(jsonObject.toJSONString());

		return "success";
	}

}
