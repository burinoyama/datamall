package org.vin.dw.publisher.service;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.vin.datamall.dw.common.constant.Constants;
import org.vin.datamall.dw.common.util.CommonUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublishServiceImpl implements PublishService {

	@Autowired
	JestClient jestClient;

	@Override
	public int getDauTotal(String date) {

		SearchSourceBuilder builder = new SearchSourceBuilder();

		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

		MatchQueryBuilder logDate = new MatchQueryBuilder("logDate", date);

		builder.query(boolQueryBuilder.filter(logDate));

		Search.Builder searchBuilder = new Search.Builder(builder.toString());

		CommonUtil.print(builder.toString());

		SearchResult execute = null;
		try {
			execute = jestClient.execute(searchBuilder.build());
		} catch (IOException e) {
			e.printStackTrace();
		}
		Integer total = execute.getTotal();
		Float maxScore = execute.getMaxScore();
		System.err.println(total);

		return total;
	}

	@Override
	public Map<String, String> getDauByHour(String date) {

		SearchSourceBuilder builder = new SearchSourceBuilder();

		TermsBuilder groupby_hour = AggregationBuilders.terms("groupby_hour").field("logHour.keyword");

		SearchSourceBuilder aggregation = builder.aggregation(groupby_hour);

		Search build = new Search.Builder(aggregation.toString()).addIndex(Constants.ES_INDEX_DAU).addType(Constants.ES_DEAFULT_TYPE).build();

		CommonUtil.print(build.toString());
		SearchResult execute = null;
		try {
			execute = jestClient.execute(build);
		} catch (IOException e) {
			e.printStackTrace();
		}

		TermsAggregation termsAggregation = execute.getAggregations().getTermsAggregation("groupby_hour");
		List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();

		Map<String, String> map = new HashMap<>();

		for (TermsAggregation.Entry bucket : buckets) {
			map.put(bucket.getKey(), bucket.getCount() + "");
		}
		return map;
	}
}
