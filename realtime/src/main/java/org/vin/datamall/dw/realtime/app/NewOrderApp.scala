package org.vin.datamall.dw.realtime.app

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.vin.datamall.dw.common.constant.Constants
import org.vin.datamall.dw.common.util.MyEsUtil
import org.vin.datamall.dw.realtime.bean.OrderInfo
import org.vin.datamall.dw.realtime.util.MyKafkaUtil

object NewOrderApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("gmaill_new_order").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val recordStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Constants.ES_INDEX_NEW_ORDER, ssc)

    val completeOrderInfoDStream: DStream[OrderInfo] = recordStream.map(_.value()).map { jsonString =>

      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

      val dateTimeArray: Array[String] = orderInfo.createTime.split(" ")

      val timeArray: Array[String] = dateTimeArray(1).split(":")

      orderInfo.createDate = dateTimeArray(0)
      orderInfo.createHour = timeArray(0)
      orderInfo.createHourMinute = timeArray(1)
      orderInfo
    }

    // DStream只能使用foreachRDD函数,不能使用foreach函数
    completeOrderInfoDStream.foreachRDD { rdd =>
      rdd.foreachPartition { orderinfoItr =>
        // es索引的三种类型:1索引+分词,2索引,3分词
        MyEsUtil.insertBulk(Constants.ES_INDEX_NEW_ORDER, orderinfoItr.toList)
      }
    }
    ssc.start
    ssc.awaitTermination
  }
}
