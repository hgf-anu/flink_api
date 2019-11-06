package com.yaxin.flink.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object MyKafkaUtil{
	private val prop = new Properties()

	//配置服务器地址，组名；不需要配置提交时机，或者管理offset
	prop.setProperty("bootstrap.servers","hadoop101:9092")
	prop.setProperty("group.id","gmall")

	def getKafkaSource(topic:String):FlinkKafkaConsumer011[String]={
		val kafkaConsumer:FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]( topic, new SimpleStringSchema(), prop )
		return kafkaConsumer
	}
}
