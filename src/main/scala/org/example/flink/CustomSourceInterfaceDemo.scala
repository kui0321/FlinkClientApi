package org.example.flink

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

class MyDefinedSource extends SourceFunction[StationLog]{
  private var isMakeData = true
  private val random = new Random()
  private val callTypes: Array[String] = Array[String]("fail", "busy", "barring", "success")

  //从数据源端加载数据，并且发送给下一个Operator算子，进行处理。 实时产生数据
  override def run(sourceContext:SourceFunction.SourceContext[StationLog]): Unit = {
    while(isMakeData){
      1.to(10).map(
        i=>{
          StationLog("sid_" + i,
            "1341234%04d".format(random.nextInt(8)),
            "1731234%04d".format(random.nextInt(8)),
            callTypes(random.nextInt(callTypes.length)),
            System.currentTimeMillis(),
            1 + random.nextInt(100))
        }
      ).foreach(ele=>{
        sourceContext.collect(ele)
      })
      Thread.sleep(5000)
    }

  }

  //当将Job作业取消时，不再从数据源端读取数据
  override def cancel(): Unit = isMakeData = false
}

class MyDefinedSource2 extends ParallelSourceFunction[StationLog]{
  private var isMakeData = true
  private val random = new Random()
  private val callTypes:Array[String] = Array[String]("fail", "busy", "barring", "success")
  override def run(sourceContext: SourceFunction.SourceContext[StationLog]): Unit = {
    while (isMakeData) {
      1.to(10).map(
        i => {
          StationLog("sid_" + i,
            "1341234%04d".format(random.nextInt(8)),
            "1731234%04d".format(random.nextInt(8)),
            callTypes(random.nextInt(callTypes.length)),
            System.currentTimeMillis(),
            1 + random.nextInt(100))
        }).foreach(ele=>{
        sourceContext.collect(ele)
      })
      Thread.sleep(5000)
    }
  }

  override def cancel(): Unit = isMakeData = false
}

object CustomSourceInterfaceDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(2)
    import org.apache.flink.streaming.api.scala._
//    val sourceFunctionDs:DataStream[StationLog] = env.addSource(new MyDefinedSource)
//    println("sourceFunctionDs并行度"+ sourceFunctionDs.parallelism)
//    sourceFunctionDs.print()
    val parallelSourceFunctionDs = env.addSource(new MyDefinedSource2)
    println("parallelSourceFunctionDs并行度"+ parallelSourceFunctionDs.parallelism)
    parallelSourceFunctionDs.print()
    env.execute("CustomSourceInterfaceDemo")
  }
}