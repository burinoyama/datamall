package org.vin.dw.publisher.service;

import java.util.Map;

public interface PublishService {

	 int getDauTotal(String date);


	 Map<String, String> getDauByHour(String date);
}
