package org.vin.datamall.dw.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Objects}

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.vin.datamall.dw.common.constant.Constants
import org.vin.datamall.dw.common.util.MyEsUtil
import org.vin.datamall.dw.realtime.bean.StartupLog
import org.vin.datamall.dw.realtime.util.{MyKafkaUtil, RedisUtil}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object StartupApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("realTime")

    val sc = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    val startUpStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Constants.KAFKA_TOPIC_STARTUP, ssc)

    startUpStream.map(_.value()).foreachRDD{rdd =>
      println(rdd.collect() mkString ("\n"))
    }

    val completeLogObject: DStream[StartupLog] = startUpStream.map(_.value()).map { log =>
      val startup: StartupLog = JSON.parseObject(log, classOf[StartupLog])
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      val logDateTime: String = dateFormat.format(new Date(startup.ts))
      val allTime: Array[String] = logDateTime.split(" ")
      startup.logDate = allTime(0)
      startup.logHourMinute = allTime(1)
      startup.logHour = startup.logHourMinute.split(":")(0)
      startup
    }

    val filteredStartUpLogDstream: DStream[StartupLog] = completeLogObject.transform { each =>
      //      println("before filter total is : " each.count())


      val jedis: Jedis = RedisUtil.getJedisClient

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

      val today: String = dateFormat.format(new Date())

      val dau_set: util.Set[String] = jedis.smembers("dau_" + today)
      val dau_bc: Broadcast[util.Set[String]] = sc.broadcast(dau_set)

      val filteredRdd: RDD[StartupLog] = each.filter { startuplog =>
        var exists = false
        if (Objects.nonNull(dau_bc.value)) {
          dau_bc.value.contains(startuplog.mid)
        }
        !exists
      }
      println("过滤后共有：" + filteredRdd.count())
      filteredRdd
    }

    //    3     把今日访问用户保存到redis中    daily active user
    //    3.1 redis的存储结构  k-v     key: "dau:"+date    value:  mid

    filteredStartUpLogDstream.foreachRDD { rdd =>
      rdd.foreachPartition { starupItr =>
        val jedis: Jedis = RedisUtil.getJedisClient

        val list = new ListBuffer[Any]

        for (startupLog <- starupItr) {
          val dauKey: String = "dau_" + startupLog.logDate
          jedis.sadd(dauKey, startupLog.mid)
          list.append(startupLog)
        }
        jedis.close()
        MyEsUtil.insertBulk(Constants.ES_INDEX_DAU, list.toList)

      }

    }
    ssc.start()
    ssc.awaitTermination()
  }


}
