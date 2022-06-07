package com.flink.datastream.flink_test

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object sink_test {
  def main(args: Array[String]): Unit = {


    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 1.从集合读取数据
    //    val dtRead: DataStream[Dtreading] = env.fromCollection(List(
    //      Dtreading("sensor_1", 1547718199, 35.8),
    //      Dtreading("sensor_6", 1547718201, 15.4),
    //      Dtreading("sensor_7", 1547718202, 6.7),
    //      Dtreading("sensor_10", 1547718205, 38.1)
    //    ) )
    //2.从文件读取数据
    //    env.readTextFile("C:\\Users\\胡丹婷\\IdeaProjects\\flink\\input\\1.txt")
    env.setParallelism(1)
    val tx: DataStream[String] = env.readTextFile("C:\\\\Users\\\\胡丹婷\\\\IdeaProjects\\\\flink\\\\input\\\\2.txt")

    //元组
    val tp: DataStream[(String, Int)] = env.fromElements(("dt", 19), ("nana", 18))
    val ss: DataStream[String] = tp.map(_._1)

    //导入Kafka  sink
    // 启动executor，执行任务
    //    val kaf: DataStreamSink[String] = ss.addSink(new FlinkKafkaProducer011[(String)]("", "", new SimpleStringSchema()))
    //Redis sink
    //    class MyRedisMapper extends RedisMapper[Dtreading]{
    //      override def getCommandDescription: RedisCommandDescription = {
    //        new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
    //      }
    //      override def getValueFromData(t: Dtreading): String = t.temperature.toString
    //
    //      override def getKeyFromData(t: Dtreading): String = t.id
    //    }
    //自定义sink 导入文件
    //    ss.addSink(StreamingFileSink.forRowFormat(
    //      new Path("C:\\Users\\胡丹婷\\IdeaProjects\\flink\\input\\3.txt"),
    //      new SimpleStringEncoder[String]("UTF-8")
    //    ).build())
    //自定义导入mysql
    //    class MyJdbcSink() extends RichSinkFunction[Dtreading] {
    //      var conn: Connection = _
    //      var insertStmt: PreparedStatement = _
    //      var updateStmt: PreparedStatement = _
    //
    //      // open 主要是创建连接
    //      override def open(parameters: Configuration): Unit = {
    //        super.open(parameters)
    //
    //        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    //        insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
    //        updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
    //      }
    //
    //      // 调用连接，执行sql
    //      override def invoke(value: Dtreading, context: SinkFunction.Context[_]): Unit = {
    //
    //        updateStmt.setDouble(1, value.temperature)
    //        updateStmt.setString(2, value.id)
    //        updateStmt.execute()
    //
    //        if (updateStmt.getUpdateCount == 0) {
    //          insertStmt.setString(1, value.id)
    //          insertStmt.setDouble(2, value.temperature)
    //          insertStmt.execute()
    //        }
    //      }
    //
    //      override def close(): Unit = {
    //        insertStmt.close()
    //        updateStmt.close()
    //        conn.close()
    //      }
    //    }

    env.execute("dt  mywife")
  }
}

