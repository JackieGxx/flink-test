package com.flink.datastream.flink_test

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object map_test {
  def main(args: Array[String]): Unit = {


    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
//    env.enableCheckpointing(10000L)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setCheckpointInterval(50000L)
//    env.getCheckpointConfig.setCheckpointTimeout(30000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(3)
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(200L)
//    env.getCheckpointConfig.setPreferCheckpointForRecovery(false)
//    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    val tx = env.addSource(new MySensorSource())
    val op: DataStream[(String, Double, Double)] = tx
      .keyBy("id")
      //      .flatMap(new flatmapTest(3.0))
      .flatMapWithState[(String, Double, Double), Double] {
        case (in: Dtreading, None) => {
          (List.empty, Some(in.temperature))
        }
        case (in: Dtreading, lastTp: Some[Double]) => {
          val diff: Double = (in.temperature - lastTp.get).abs
          if (diff > 3.0) {
            (List((in.id, in.temperature, lastTp.get)), Some(in.temperature))
          } else {
            (List.empty, Some(in.temperature))
          }
        }
      }

    op.print("温度差异")

    env.execute("ok temp")
  }
}

class mapTest(threshold: Double) extends RichMapFunction[Dtreading, (String, Double, Double)] {
  var tempChange: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    tempChange = getRuntimeContext.getState(new ValueStateDescriptor[Double]("temp", classOf[Double]))

  }

  override def map(in: Dtreading): (String, Double, Double) = {
    //取出上次温度值
    val lastTemp: Double = tempChange.value()
    //更新状态
    tempChange.update(in.temperature)
    //和当前温度取差值，如果大于阈值，就报警
    val diff: Double = (in.temperature - lastTemp).abs
    if (diff > threshold) {
      (in.id, in.temperature, lastTemp)
    } else {
      (in.id, -0.1, -0.1)
    }

  }
}

class flatmapTest(thr: Double) extends RichFlatMapFunction[Dtreading, (String, Double, Double)] {
  //定义上次的温度
  var flatTemp: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    flatTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("faltmap-test", classOf[Double]))

  }

  override def flatMap(in: Dtreading, collector: Collector[(String, Double, Double)]): Unit = {
    //取出上次的温度值
    val lastTemp: Double = flatTemp.value()
    //更新状态
    flatTemp.update(in.temperature)
    //和当前温度取差值，如果大于阈值，就报警
    val diff: Double = (in.temperature - lastTemp).abs
    if (diff > thr) {
      collector.collect((in.id, in.temperature, lastTemp))
    }
  }
}