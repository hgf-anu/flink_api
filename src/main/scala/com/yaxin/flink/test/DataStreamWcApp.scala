package com.yaxin.flink.test

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object DataStreamWcApp{

	def main(args:Array[String]):Unit ={
		//1、流式处理和有界处理的环境变量前面就是加上Stream
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//2.测试Socket端口的通信，使用nc 7777来输入数据
		val dataStream: DataStream[String] = env.socketTextStream("hadoop102",7777)

		import org.apache.flink.api.scala._
		val sumDstream:DataStream[(String, Int)] = dataStream.flatMap( _.split( " " ) ).filter( _.nonEmpty ).map( (_, 1) ).keyBy(
			0 ).sum( 1 )
		sumDstream.print()

		//3.最后必须执行，类似Spark的awit
		env.execute()
	}
}
