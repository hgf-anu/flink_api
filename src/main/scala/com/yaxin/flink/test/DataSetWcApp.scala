package com.yaxin.flink.test

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}



/**
 * 处理离线数据是DataSet
 * Table API类似与Spark的DSL风格Sql语句
 */
object DataSetWcApp{

	def main(args:Array[String]):Unit ={
		//1.env 类似与spark的sparkContext；注意选对应版本的环境，这里是scala的
		val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

		//2.source
		//有界的数据集,类似与RDD【String】
		val txtDataSet: DataSet[String] = env.readTextFile("D:\\temp\\hello.txt")

		//3.transform
		//使用flatMap需要先进行隐式转换，不然会报错，IDEA版本高了会自动提示
		import org.apache.flink.api.scala._
		//返回一个聚合的DataSet
		val aggSet:AggregateDataSet[(String, Int)] = txtDataSet.flatMap( _.split( " " ) ).map( (_, 1) ).groupBy( 0 ).sum(
			1 )

		//4.sink 类似与spark的行动算子。写的目的地可以有很多，比如MySQL、ES、HBase、Redis。官方提供很多客户端工具：Kafka、。准确捕获处理的状态，如果sink结束就代表成功，exactly once在这里体现在flink是以文件传输后才反馈sink成功。
		aggSet.print()
	}
}
