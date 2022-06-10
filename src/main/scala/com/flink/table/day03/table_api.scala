package com.flink.table.day03

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment

object table_api {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env: ExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inDS: DataStream[String] = env.readTextFile("C:\\Users\\胡丹婷\\IdeaProjects\\flink\\input\\1.txt")

    //创建表的执行环境
    val tableEnv:StreamTableEnvironment = StreamTableEnvironment.create(env)
    //基于数据流，转换成一张表，然后进行操作
    val dataTable: Table = tableEnv.fromDataStream(tx)
    env.execute("table job" )
  }
}
