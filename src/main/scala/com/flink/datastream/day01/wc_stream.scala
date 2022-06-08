package com.flink.datastream.day01

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object wc_stream {
  def main(args: Array[String]): Unit = {
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = tool.get("host")
    val port = tool.get("port").toInt

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 接收socket文本流
    val socketDS: DataStream[String] = env.socketTextStream(host, port)
    // flatMap和Map需要引用的隐式转换
    val res: DataStream[(String, Int)] = socketDS.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(_._1).sum(1)
    // 打印输出
    res.print().setParallelism(1)
    // 启动executor，执行任务
    env.execute("wordcount job")
  }
}
