package org.vin.mall;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.vin.mall.sender.LogUploader;
import org.vin.mall.util.RanOpt;
import org.vin.mall.util.RandomDate;
import org.vin.mall.util.RandomNum;
import org.vin.mall.util.RandomOptionGroup;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;


public class JsonMocker {

	int startupNum = 100000;
	int eventNum = 200000;
	String appId = "datamall";

	RandomDate logDateUtil = null;

	public JsonMocker() {

	}


	RanOpt[] osOpts = {new RanOpt("ios", 3), new RanOpt("andriod", 7)};
	RandomOptionGroup<String> osOptionGroup = new RandomOptionGroup(osOpts);
	Date startTime = null;
	Date endTime = null;

	RanOpt[] areaOpts = {new RanOpt("beijing", 10),
			new RanOpt("shanghai", 10), new RanOpt("guangdong", 20), new RanOpt("hebei", 5),
			new RanOpt("heilongjiang", 5), new RanOpt("shandong", 5), new RanOpt("tianjin", 5),
			new RanOpt("shan3xi", 5), new RanOpt("shan1xi", 5), new RanOpt("sichuan", 5)
	};
	RandomOptionGroup<String> areaOptionGroup = new RandomOptionGroup(areaOpts);


	RanOpt[] vsOpts = {new RanOpt("1.2.0", 50), new RanOpt("1.1.2", 15),
			new RanOpt("1.1.3", 30),
			new RanOpt("1.1.1", 5)
	};

	RandomOptionGroup<String> vsOptionGroup = new RandomOptionGroup(vsOpts);

	RanOpt[] eventOpts = {new RanOpt("addFavor", 10), new RanOpt("addComment", 30),
			new RanOpt("addCart", 20), new RanOpt("clickItem", 40)
	};

	RandomOptionGroup<String> eventOptionGroup = new RandomOptionGroup(eventOpts);

	RanOpt[] channelOpts = {new RanOpt("xiaomi", 10), new RanOpt("huawei", 20),
			new RanOpt("wandoujia", 30), new RanOpt("360", 20), new RanOpt("tencent", 20)
			, new RanOpt("baidu", 10), new RanOpt("website", 10)
	};

	RandomOptionGroup<String> channelOptionGroup = new RandomOptionGroup(channelOpts);

	RanOpt[] quitOpts = {new RanOpt(true, 50), new RanOpt(false, 60)};

	RandomOptionGroup<Boolean> isQuitGroup = new RandomOptionGroup(quitOpts);



	public JsonMocker(String startTimeString, String endTimeString, int startupNum, int eventNum) {
		try {
			startTime = new SimpleDateFormat("yyyy-MM-dd").parse(startTimeString);
			endTime = new SimpleDateFormat("yyyy-MM-dd").parse(endTimeString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		logDateUtil = new RandomDate(startTime, endTime, startupNum + eventNum);
	}

	String initEventLog(String startLogJson) {
            /*`type` string   COMMENT '日志类型',
             `mid`   string   COMMENT '设备唯一 表示',
            `uid`    string   COMMENT '用户标识',
            `os`     string   COMMENT '操作系统',
            `appid`  string   COMMENT '应用id',
            `area`   string   COMMENT '地区' ,
            `evid`   string   COMMENT '事件id',
            `pgid`   string   COMMENT '当前页',
            `npgid`  string   COMMENT '跳转页',
            `itemid` string   COMMENT '商品编号',
            `ts`     bigint   COMMENT '时间',*/

		JSONObject startLog = JSON.parseObject(startLogJson);
		//  long ts= logDateUtil.getRandomDate().getTime();

		JSONObject jsonObject = new JSONObject();
		jsonObject.put("type",  "event");
		jsonObject.put("mid",   startLog.getString("mid"));
		jsonObject.put("uid",   startLog.getString("uid"));
		jsonObject.put("os",    startLog.getString("os"));
		jsonObject.put("appid", this.appId);
		jsonObject.put("area",  startLog.getString("area"));
		jsonObject.put("evid",  eventOptionGroup.getRandomOpt().getValue());
		jsonObject.put("pgid",  new Random().nextInt(50) + 1);
		jsonObject.put("npgid", new Random().nextInt(50) + 1);
		jsonObject.put("itemid", new Random().nextInt(50));
		return jsonObject.toJSONString();
	}


	String initStartupLog() {
            /*`type` string   COMMENT '日志类型' :startup   event,
             `mid` string COMMENT '设备唯一标识',
      `uid` string COMMENT '用户标识',
      `os` string COMMENT '操作系统', ,
      `appId` string COMMENT '应用id', ,
     `vs` string COMMENT '版本号',
     `ts` bigint COMMENT '启动时间',
     `area` string COMMENT '地区'
          `ch` string COMMENT '渠道'
     */


		String mid = "mid_" + RandomNum.getRandInt(1, 500);
		String uid = "" + RandomNum.getRandInt(1, 500);
		String os = osOptionGroup.getRandomOpt().getValue();
		String appid = this.appId;
		String area = areaOptionGroup.getRandomOpt().getValue();
		String vs = vsOptionGroup.getRandomOpt().getValue();
		//long ts= logDateUtil.getRandomDate().getTime();
		String ch = os.equals("ios") ? "appstore" : channelOptionGroup.getRandomOpt().getValue();


		JSONObject jsonObject = new JSONObject();
		jsonObject.put("type", "startup");
		jsonObject.put("mid", mid);
		jsonObject.put("uid", uid);
		jsonObject.put("os", os);
		jsonObject.put("appid", appid);
		jsonObject.put("area", area);
		jsonObject.put("ch", ch);
		jsonObject.put("vs", vs);
		return jsonObject.toJSONString();
	}

	public static void genLog() {
		JsonMocker jsonMocker = new JsonMocker();
		jsonMocker.startupNum = 1000000;
		for (int i = 0; i < jsonMocker.startupNum; i++) {
			String startupLog = jsonMocker.initStartupLog();
			jsonMocker.sendLog(startupLog);
			while (jsonMocker.isQuitGroup.getRandomOpt().getValue()) {
				String eventLog = jsonMocker.initEventLog(startupLog);
				jsonMocker.sendLog(eventLog);
			}
		}


	}


	public void sendLog(String log) {
		LogUploader.sendLogStream(log);
	}


	public static void main(String[] args) {
		genLog();
	}

}


