package org.example.flink.source.kafka

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

object KafkaSourceDemo {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val kafkaSource:KafkaSource[String] = KafkaSource.builder[String]()
      .setBootstrapServers("node2:9092,node3:9092,node4:9092")
      .setTopics("flink-topic1")
      .setGroupId("flinkgroup1")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
    val dataDs:DataStream[String] = env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"KafkaSource")
    dataDs.print()
    env.execute("KafkaSourceDemo")
  }
}
