package com.flink.datastream.flink_test

import java.util

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object state_test {
  def main(args: Array[String]): Unit = {


    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.setStateBackend(new MemoryStateBackend())
    //    env.setStateBackend(new FsStateBackend(""))
    //
    //    env.setStateBackend(new RocksDBStateBackend(""))
    val tx = env.addSource(new MySensorSource())


    env.execute("ok temp")
  }
}

class dtstate extends KeyedProcessFunction[String, Dtreading, Int] {
  //ListStateDescriptor
  lazy val list_TEST: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("List-test", classOf[Int]))
  //MapStateDescriptor
  lazy val mapTest: MapState[String, Int] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Int]("Map-Tset", classOf[String], classOf[Int]))
  //ReducingStateDescriptor
  lazy val reduce_Tset: ReducingState[Dtreading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Dtreading]("reduce-Test", new ReduceFunction[Dtreading] {
    override def reduce(t: Dtreading, t1: Dtreading): Dtreading = {
      Dtreading(t1.id, t.timestamp.min(t1.timestamp), t.temperature.max(t1.temperature))
    }
  }, classOf[Dtreading]))
  //
  getRuntimeContext.getReducingState(new ReducingStateDescriptor[String]("red", new ReduceFunction[String] {
    override def reduce(t: String, t1: String): String = {
      t
    }
  }, classOf[String]))
  // lazy val va: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("Value-state", classOf[Int]))
  var va: ValueState[Int] = _

  override def open(parameters: Configuration): Unit = {
    va = getRuntimeContext.getState(new ValueStateDescriptor[Int]("Value-state", classOf[Int]))
  }

  override def processElement(i: Dtreading, context: KeyedProcessFunction[String, Dtreading, Int]#Context, collector: Collector[Int]): Unit = {
    va.update(1)
    va.value()
    va.clear()
    list_TEST.add(1)
    list_TEST.get()
    list_TEST.update(new util.LinkedList[Int]())
    mapTest.contains("a")
    reduce_Tset.add(Dtreading("2", 11L, 3.3))

  }
}