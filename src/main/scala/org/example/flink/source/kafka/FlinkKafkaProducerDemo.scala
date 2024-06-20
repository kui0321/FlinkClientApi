package org.example.flink.source.kafka

import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord
import org.codehaus.jackson.map.ser.std.StdArraySerializers.ByteArraySerializer

import java.lang
import java.util.Properties

object FlinkKafkaProducerDemo {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream:DataStream[String] = env.socketTextStream("node3",8888)
    val result:DataStream[(String,Int)] = stream.flatMap(_.split("\\s+")).map((_,1))
      .keyBy(_._1).sum(1)
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","node3:9092,node4:9092,node5:9092")
    prop.setProperty("key.serializer",classOf[ByteArraySerializer].getName)
    prop.setProperty("value.serializer",classOf[ByteArraySerializer].getName)
    val schema = new KafkaSerializationSchema[(String,Int)]{
      override def serialize(element: (String, Int), timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord("topic_sink",element._1.getBytes,(element._2+"").getBytes())
      }
    }
    val producer = new FlinkKafkaProducer[(String,Int)](
      "topic_sink",
      schema,
      prop,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    )
    result.addSink(producer)
    env.execute("WordCountStream")

  }
}
