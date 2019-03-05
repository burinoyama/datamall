package org.vin.datamall.dw.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.google.common.base.CaseFormat;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.vin.datamall.dw.common.constant.Constants;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

	public static void main(String[] args) {

		CanalConnector canalConnector =
				CanalConnectors.newSingleConnector(
						new InetSocketAddress("hadoop6", 11111), "example", "", "");

		while (true) {
			canalConnector.connect();
			canalConnector.subscribe("gmall.order.info");

			Message message = canalConnector.get(100);

			System.err.println("增量" + message.getEntries() + "个sql");
			List<CanalEntry.Entry> entries = message.getEntries();

			if (entries.size() == 0) {
				System.err.println("没有发现增量数据");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				for (CanalEntry.Entry entry : entries) {
					if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
						CanalEntry.RowChange rowChange = null;
						try {
							rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
						} catch (InvalidProtocolBufferException ex) {
							ex.printStackTrace();
						}
						List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
						CanalEntry.EventType eventType = rowChange.getEventType();
						String tableName = entry.getHeader().getTableName();
						handle(tableName, eventType, rowDatasList);

					}

				}
			}

		}
	}

	public static void handle(String tableName, CanalEntry.EventType type, List<CanalEntry.RowData> rowDatasList) {
		if (Constants.TABLE_NAME_ORDER_INFO.equals(tableName) && type == CanalEntry.EventType.INSERT && rowDatasList.size() > 0) {
			JSONObject jsonObject = new JSONObject();
			for (CanalEntry.RowData rowData : rowDatasList) {
				List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
				for (CanalEntry.Column column : afterColumnsList) {

					String columnNameCamel = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
					String columnValueCamel = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getValue());

					jsonObject.put(columnNameCamel, columnValueCamel);
					System.err.println(jsonObject.toJSONString());

				}

			}
		}
	}

}
