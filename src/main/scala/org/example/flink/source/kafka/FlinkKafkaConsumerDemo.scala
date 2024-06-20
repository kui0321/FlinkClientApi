package org.example.flink.source.kafka

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object FlinkKafkaConsumerDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node2:9092,node3:9092,node4:9092")
    prop.setProperty("group.id", "flinkgroup1")
    import org.apache.flink.streaming.api.scala._
    val dataDs:DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](
      "flink-topic1",
      new SimpleStringSchema(),
      prop
    ))
    //dataDs.print()
    dataDs.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
      .print()
    env.execute("FlinkKafkaConsumerDemo")
  }
}
