package com.yaxin.flink.api

import com.yaxin.flink.util.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object StreamApiApp{
	def main(args:Array[String]):Unit ={
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val kafkaSource: FlinkKafkaConsumer011[String] = MyKafkaUtil.getKafkaSource("GAMLL_STARTUP")
		import org.apache.flink.api.scala._
		val dstream:DataStream[String] = env.addSource( kafkaSource )
		dstream.print()

		env.execute()
	}
}
