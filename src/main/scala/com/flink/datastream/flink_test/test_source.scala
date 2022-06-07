package com.flink.datastream.flink_test

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

import scala.util.Random

object test_source {
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
    val mp: DataStream[Dtreading] = tx.map(
      data => {
        val sp: Array[String] = data.split(",")
        Dtreading(sp(0), sp(1).toLong, sp(2).toDouble)
      }
    )
      //      .keyBy(_.id).maxBy(2) maxBy以这个字段排序，其中的值还是当前的值。而max是最小值，其中的值是第一个值。
      .keyBy(0)
      //      .reduce(
      //        (cur, newData) => {
      //          Dtreading(cur.id, cur.timestamp.max(newData.timestamp), cur.temperature.min(newData.temperature))
      //        }
      //      )
      .reduce(new dtreduce) //可以输一个函数类
    //分流 Split 和 Select
    val spt: SplitStream[Dtreading] = mp.split(
      line => {
        if (line.temperature > 60) {
          List("高了")
        }
        else {
          List("低了")
        }

      }
    )

    val high: DataStream[Dtreading] = spt.select("高了")
    val low: DataStream[Dtreading] = spt.select("低了")
    val all: DataStream[Dtreading] = spt.select("高了", "低了")

    // 合流 Connect和 CoMap 只能2个
    val highTemp: DataStream[(String, Double)] = high.map(
      data => {
        (data.id, data.temperature)
      }
    )
    val conn: ConnectedStreams[(String, Double), Dtreading] = highTemp.connect(low)
    val comap = conn.map(
      highdata => {
        (highdata._1, highdata._2, "温度太高了")
      },
      lowdata => {
        (lowdata.temperature, "还凑合")
      }
    )
    //union 可以联合多个
    val un: DataStream[Dtreading] = high.union(low, all)
    //元组
    val tp: DataStream[(String, Int)] = env.fromElements(("dt", 19), ("nana", 18))
    val ss: DataStream[String] = tp.map(_._1)
    //3.以kafka消息队列的数据作为来源
    //    val properties = new Properties()
    //    properties.setProperty("bootstrap.servers", "localhost:9092")
    //    properties.setProperty("group.id", "consumer-group")
    //    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    properties.setProperty("auto.offset.reset", "latest")
    //
    //    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    //4.自定义Source
    //    val stream4 = env.addSource(new MySensorSource())

    // 打印输出
    //    mp.print("dt love")

    //    high.print("H")
    //    low.print("L")
    //    all.print("all")
    //    comap.print("res")
    //    un.print("result")

    //导入Kafka  sink
    // 启动executor，执行任务
    //    val kaf: DataStreamSink[String] = ss.addSink(new FlinkKafkaProducer011[(String)]("", "", new SimpleStringSchema()))

    env.execute("dt  mywife")
  }
}

// 定义样例类，传感器id，时间戳，温度
case class Dtreading(id: String, timestamp: Long, temperature: Double)

//自定义Source
class MySensorSource extends SourceFunction[Dtreading] {

  // flag: 表示数据源是否还在正常运行
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[Dtreading]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()

    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, 65 + rand.nextGaussian() * 20)
    )

    while (running) {
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()

      curTemp.foreach(
        t => ctx.collect(Dtreading(t._1, curTime, t._2))
      )
      Thread.sleep(100)
    }
  }
}

class dtreduce extends ReduceFunction[Dtreading] {
  override def reduce(t: Dtreading, t1: Dtreading): Dtreading = {
    Dtreading(t.id, t.timestamp.max(t1.timestamp), t.temperature.min(t1.temperature))
  }
}

class dtmap extends RichMapFunction[Dtreading, Int] {
  //  override def open(parameters: Configuration): Unit = {}

  override def map(in: Dtreading): Int = {
    in.temperature.toInt
  }

  //  getRuntimeContext.getState()
  //  override def close(): Unit = {}
}