package com.flink.datastream.flink_test

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object windows_test {
  def main(args: Array[String]): Unit = {


    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //元组
    //    val tp: DataStream[(String, Int)] = env.fromElements(("dt", 19), ("nana", 18))
    //    tp.keyBy(_._1).timeWindow(Time.seconds(10), Time.seconds(15))
    //    tp.keyBy(_._1).window(EventTimeSessionWindows.withGap(Time.seconds(10)))
    //    tp.keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)))//第二参数是偏移量
    //    tp.keyBy(_._1).countWindow(10, 2)
    //    tp.keyBy(_._1).timeWindow(Time.seconds(20),Time.seconds(10))
    //    tp.keyBy(_._1).assignAscendingTimestamps(_._2.toLong * 1000L)
    //    如果我们事先得知数据流的时间戳是单调递增的，也就是说没有乱序->assignAscendingTimestamps
    val tx = env.addSource(new MySensorSource())
    val mp: DataStream[Dtreading] = tx.map(
      data => data
    )
    val wk: DataStream[Dtreading] = mp.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Dtreading](Time.seconds(2)) {
      override def extractTimestamp(t: Dtreading): Long = { //提取时间戳
        //10位数是秒，13位是毫秒
        t.timestamp
      }
    })
    //
    val op= wk.keyBy(_.id).timeWindow(Time.seconds(15), Time.seconds(5))
      .allowedLateness(Time.seconds(10))
      .sideOutputLateData(new OutputTag[Dtreading]("晚了"))
//      .reduce(new dtreduce)
      .apply(new apdtWin())
    //
    val er: DataStream[Dtreading] = op.getSideOutput(new OutputTag[Dtreading]("晚了"))
    op.print("we")
    env.execute("ok")
  }
}
//全窗口函数 如果是“id”，那是tuple格式
class apdtWin extends WindowFunction[Dtreading,(Long,Int),String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Dtreading], out: Collector[(Long,Int)]): Unit = {
    out.collect(window.getStart,input.size)

  }
}
