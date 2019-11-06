package com.yaxin.flink.test

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}


object DataSet2WcApp{

	def main(args:Array[String]):Unit ={
		if(args.length!=4){
			println( args.length )
			println( "参数个数不正确" )
			System.exit(1)
		}
		//Flink提供的一个系统参数读取类，把参数当成一个键值对来读取
		val tool: ParameterTool = ParameterTool.fromArgs(args)

		val inputPath: String = tool.get("input")
		val outputPath: String = tool.get("output")
		//设置参数的形式是：--input 路径 --output 路径
		val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

		//使用inputPath代替原来写死的路径
		val txtDataSet: DataSet[String] = env.readTextFile(inputPath)

		import org.apache.flink.api.scala._
		val aggSet:AggregateDataSet[(String, Int)] = txtDataSet.flatMap( _.split( " " ) ).map( (_, 1) ).groupBy( 0 ).sum(
			1 )

		//可以控制并行度，默认是12;所有的算子都可以设置并行度。
		aggSet.writeAsCsv(outputPath).setParallelism(1)
		//如果是打印等操作不需要执行，如果要写入到文件中需要加入执行
		env.execute()
		//./flink run -c com.yaxin.flink.DataSet2WcApp /opt/module/flink-1.7.0/lib/flink1205-1.0-SNAPSHOT.jar --input ~/applog/flink/input.txt --output ~/applog/flink/output2019-11-06.csv
	}
}
