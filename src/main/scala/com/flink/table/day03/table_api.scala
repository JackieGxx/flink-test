package com.flink.table.day03

import com.flink.datastream.flink_test.Dtreading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}


object table_api {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val input: DataStream[String] = env.readTextFile("C:\\Users\\胡丹婷\\IdeaProjects\\flink\\input\\2.txt")
val input: DataStream[String] = env.socketTextStream("DtMyLover", 7777)
    val dataS: DataStream[Dtreading] = input.map(
      line => {
        val arr: Array[String] = line.split(",")
        Dtreading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )
    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //基于数据流，转换成1张表，然后进行操作
    val dataTable: Table = tableEnv.fromDataStream(dataS)
    //调用Table Api,得到转换结果
    val res: Table = dataTable.select("id,temperature")
    val rt: Table = tableEnv.sqlQuery("select id,  temperature from " + dataTable + " where id = 'sensor_9' ")
    //
    //    res.printSchema()
    val resT: DataStream[(String, Double)] = rt.toAppendStream
    resT.print("resttttt")

    env.execute("nbbb")
  }
}
