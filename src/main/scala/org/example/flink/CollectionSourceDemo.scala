package org.example.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/** 通信基站日志数据
 *
 * @param sid      基站ID
 * @param callOut  主叫号码
 * @param callIn   被叫号码
 * @param callType 通话类型eg:呼叫失败(fail)，占
 *                 线(busy),拒接（barring），接通(success):
 * @param callTime 呼叫时间戳，精确到毫秒
 * @Param duration 通话时长 单位：秒
 */
case class StationLog(sid:String, callOut:String, callIn:String, callType:String, callTime:Long, duration:Long)

object CollectionSourceDemo {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.api.scala._
    val eleIntDs:DataStream[Int] = env.fromElements(1,2,3,4)
    val numDs:DataStream[Long] = env.fromSequence(1,10)
    val collCaseObjDs:DataStream[StationLog] = env.fromCollection(Array[StationLog](
      StationLog("1", "18612345670", "17312345679",
        "success", System.currentTimeMillis(), 86L),
      StationLog("2", "18612345671", "17312345678",
        "success", System.currentTimeMillis(), 86L),
      StationLog("3", "18612345672", "17312345677",
        "busy", System.currentTimeMillis(), 86L),
      StationLog("4", "18612345673", "17312345676",
        "success", System.currentTimeMillis(), 86L),
      StationLog("5", "18612345674", "17312345675",
        "busy", System.currentTimeMillis(), 86L),
    ))
    val collStringDs:DataStream[String] =  env.fromCollection(Array[String](
      "01,18612345670,17312345679,success," + System.currentTimeMillis() + ",96",
           "02,18612345671,17312345678,success," + System.currentTimeMillis() + ",95",
           "03,18612345672,17312345677,success," + System.currentTimeMillis() + ",94",
           "04,18612345673,17312345676,success," + System.currentTimeMillis() + ",93",
           "05,18612345674,17312345675,success," + System.currentTimeMillis() + ",92"
    ))
    val resultDs:DataStream[StationLog] = collStringDs.map(
      ele => {
        val arr:Array[String] = ele.split(",")
        StationLog(arr(0),arr(1),arr(2),arr(3),arr(4).toLong,arr(5).toLong)
      })
    resultDs.print()
    env.execute("CollectionSourceDemo")
  }
}
