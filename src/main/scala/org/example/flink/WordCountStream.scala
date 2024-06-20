package org.example.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
 * WordCountStream对象实现了通过流处理计算文本中单词出现次数的示例。
 * 它使用Flink的流处理API来从指定的socket流中读取数据，进行单词计数，并打印结果。
 */
object WordCountStream {

  /**
   * 程序的入口点。
   * @param args 命令行参数，目前未使用。
   */
  def main(args: Array[String]): Unit = {
    // 获取流执行环境，这是Flink流处理程序的起点。
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度为1，确保程序顺序执行，便于调试。
    env.setParallelism(1)

    // 导入Flink的Scala API，用于操作DataStream。
    import org.apache.flink.streaming.api.scala._

    // 从指定的socket地址和端口读取文本流。
    // 这里的socketTextStream用于模拟数据输入，通常在实际场景中会连接到实际的数据源。
    val stream: DataStream[String] = env.socketTextStream("node3", 8888)

    // 对文本流进行单词计数的处理流程：
    // 1. 使用flatMap将每行文本拆分成单词。
    // 2. 使用map为每个单词生成一个元组，形式为(word, 1)，表示该单词出现一次。
    // 3. 使用keyBy将数据按照单词进行分组，为每个分组内的单词进行累加。
    // 4. 使用sum对每个分组内的单词出现次数进行累加。
    val result: DataStream[(String, Int)] = stream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    // 将结果打印到控制台。
    result.print()

    // 启动Flink作业执行。
    env.execute("wordcount")
  }
}
