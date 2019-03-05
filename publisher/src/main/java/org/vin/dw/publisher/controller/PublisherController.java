package org.vin.dw.publisher.controller;


import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.vin.dw.publisher.service.PublishService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {


	@Autowired
	PublishService publishService;

	@GetMapping("realtime-total")
	public String getRealTimeTotal(@RequestParam("date") String date) {
		int dauTotal = publishService.getDauTotal(date);
		List<Map<String,String>> totalList = new ArrayList<>();

		Map<String,String> dauMap = new HashMap<>();

		dauMap.put("id","dau");
		dauMap.put("name","每日日活");
		dauMap.put("value",dauTotal + "");
		totalList.add(dauMap);

		Map<String, String> dauByHour = publishService.getDauByHour(date);
		totalList.add(dauByHour);

		return JSON.toJSONString(totalList);

	}
}
