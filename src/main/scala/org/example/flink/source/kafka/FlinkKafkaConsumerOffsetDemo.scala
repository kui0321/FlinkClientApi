package org.example.flink.source.kafka

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

import java.util
import java.util.Properties

object FlinkKafkaConsumerOffsetDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val prop:Properties = new Properties()
    prop.setProperty("bootstrap.servers", "node2:9092,node3:9092,node4:9092")
    prop.setProperty("group.id", "flinkgroup1")
    val kafkaConsumer = new FlinkKafkaConsumer[String]("flink-topic1",new SimpleStringSchema,prop)
//    //TODO 1.Flink从topic中现存的数据里最小位置开始消费
//    kafkaConsumer.setStartFromEarliest()
//    //TODO 2.Flink从topic中最新的数据开始消费
//    kafkaConsumer.setStartFromLatest()
//    //TODO 3.Flink从topic中指定group上次消费的位置开始消费，必须配置group.id参数
//    kafkaConsumer.setStartFromGroupOffsets()
//    //TODO 4.Flink从topic中指定时间戳
//    kafkaConsumer.setStartFromTimestamp(1671609649341L)
    //TODO 5.Flink从topic的分区指定具体的偏移量
    val offsets = new util.HashMap[KafkaTopicPartition,java.lang.Long]()
    offsets.put(new KafkaTopicPartition("flink-topic1",0),2L)
    offsets.put(new KafkaTopicPartition("flink-topic1",1),3L)
    offsets.put(new KafkaTopicPartition("flink-topic1",2),2L)
    kafkaConsumer.setStartFromSpecificOffsets(offsets)
    import org.apache.flink.streaming.api.scala._
    val dataDs = env.addSource(kafkaConsumer)
    dataDs.print()
    env.execute("FlinkKafkaConsumerOffsetDemo")
  }
}
