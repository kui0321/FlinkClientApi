package org.example.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


object FlieSourceDemo {
  def main(args: Array[String]): Unit = {
    val env :StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //默认按照当前机器的CPU核数并行运行
    env.setParallelism(1)
    val txtDs: DataStream[String] = env.readTextFile("D:\\BIgdate\\bigdateinfo\\Flink\\软件\\datas\\words")
//    txtDs.flatMap(_.split("\\s+"))
//      .map((_,1))
//      .keyBy(_._1)
//      .sum(1)
//
//    txtDs.print()
    txtDs.printToErr()
    val gzDs: DataStream[String] = env.readTextFile("D:\\BIgdate\\bigdateinfo\\Flink\\软件\\datas\\words.gz")
    gzDs.print()
    env.execute("FlieSourceDemo")
  }
}
