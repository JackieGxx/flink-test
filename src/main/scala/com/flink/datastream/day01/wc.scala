package com.flink.datastream.day01

import org.apache.flink.api.scala._

object wc {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inDS: DataSet[String] = env.readTextFile("C:\\Users\\胡丹婷\\IdeaProjects\\flink\\input\\1.txt")
    // 分词之后，对单词进行groupby分组，然后用sum进行聚合
    val wcSum = inDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    // 打印输出
    wcSum.print()
  }
}
