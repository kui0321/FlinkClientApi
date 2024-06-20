package org.example.flink



/**
 * WordCountBatch 对象实现了使用 Apache Flink 进行批处理单词计数的示例。
 * 它读取一个文本文件，计算每个单词出现的次数，然后输出结果。
 */
object WordCountBatch {
  /**
   * 程序的入口点。
   * @param args 命令行参数，预期为输入文件的路径。
   */
  def main(args: Array[String]): Unit = {
    // 导入 Flink 的 Scala API
    import org.apache.flink.api.scala._

    // 获取执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 从指定路径读取文本文件作为数据源
    val dataSource: DataSet[String] = env.readTextFile("datas/words")

    // 对文本进行分词，然后为每个词生成一个 (word, 1) 的元组
    dataSource.flatMap(_.split("\\s+"))
      // 为每个词生成计数为1的元组
      .map((_, 1))
      // 按单词进行分组，准备进行求和操作
      .groupBy(0)
      // 对每个单词的计数进行求和
      .sum(1)
      // 打印结果
      .print()
  }
}

