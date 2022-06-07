package com.flink.datastream.flink_test

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object process_test {
  def main(args: Array[String]): Unit = {


    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val tx = env.addSource(new MySensorSource())
    val mp: DataStream[Dtreading] = tx.map(
      data => data
    )
    //侧输出流
    val outS: DataStream[Dtreading] = mp.process(new sideOut(30.0))
    //侧输出流
    val lowtemp: DataStream[(String, Long, Double)] = outS.getSideOutput(new OutputTag[(String, Long, Double)]("LowTemp"))
    //
    val op = mp.keyBy("id")
      .process(new dtprco(10000L)) //process和window函数不要同时用
    //Flink提供了8个Process Function
    //    op.print("温度报警")
    //
    outS.print("high")
    lowtemp.print("low")

    env.execute("ok temp")
  }
}

class dtprco(interval: Long) extends KeyedProcessFunction[Tuple, Dtreading, String] {
  //需要和之前的温度做对比，所以将上一个温度保存成状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("LAST", classOf[Double]))
  //为了方便删除定时器，还需要保存定时器的时间戳
  lazy val curTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-Timer", classOf[Long]))

  override def processElement(i: Dtreading, context: KeyedProcessFunction[Tuple, Dtreading, String]#Context, collector: Collector[String]): Unit = {
    //首先取出状态
    val lastTemp: Double = lastTempState.value()
    val curTimeTs: Long = curTimerState.value()
    //将上次温度的状态更新为当前数据的温度值
    lastTempState.update(i.temperature)
    //判断当前温度值，如果比之前温度高，并且没有定时器的话，注册10秒后的定时器
    if (i.temperature > lastTemp && curTimeTs == 0) {
      val ts: Long = context.timerService().currentProcessingTime() + interval //注册10秒后的定时器
      //定义定时器，注册定时器
      context.timerService().registerProcessingTimeTimer(ts)
      curTimerState.update(ts)
    }
    else if (i.temperature < lastTemp) {
      context.timerService().deleteProcessingTimeTimer(curTimeTs)
      curTimerState.clear()
    }
  }

  //触发定时器，说明10秒内没有温度下降，报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, Dtreading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("温度在" + interval / 1000 + "，秒内一直上升")
    curTimerState.clear()
  }

}

class sideOut(thread: Double) extends ProcessFunction[Dtreading, Dtreading] {
  override def processElement(i: Dtreading, context: ProcessFunction[Dtreading, Dtreading]#Context, collector: Collector[Dtreading]): Unit = {
    if (i.temperature > thread) {
      collector.collect(i)
    } else {
      context.output(new OutputTag[(String, Long, Double)]("LowTemp"), (i.id, i.timestamp, i.temperature))
    }
  }
}