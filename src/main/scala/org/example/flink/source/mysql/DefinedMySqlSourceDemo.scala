package org.example.flink.source.mysql

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.concurrent.TimeUnit

case class Product(id: Int, name: String, price: Double)

class MySQLSource extends RichSourceFunction[Product]{
  private var flag = true
  private var conn:Connection = _
  private var pstat:PreparedStatement = _
  private var resuleSet:ResultSet = _

  override def open(parameters: Configuration): Unit = {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
    }
    conn = DriverManager.getConnection("jdbc:mysql://node1:3306/flink_db?useSSL=false", "root", "123456")
    pstat = conn.prepareStatement("select id,name,price from tb_product")
  }

  override def run(sourceContext: SourceFunction.SourceContext[Product]): Unit = {
    while (flag) {
      resuleSet = pstat.executeQuery()
      while(resuleSet.next()){
        var product:Product = Product(resuleSet.getInt("id"),
          resuleSet.getString("name"),
          resuleSet.getDouble("price"))
        sourceContext.collect(product)
      }
      TimeUnit.SECONDS.sleep(5)
    }
  }

  override def cancel(): Unit = flag = false

  override def close(): Unit = {
    if (resuleSet != null) {
      resuleSet.close()
    }
    if (pstat != null) {
      pstat.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
}

object DefinedMySqlSourceDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.api.scala._
    val productDS: DataStream[Product] = env.addSource(new MySQLSource)
    productDS.print()
    env.execute("mysql def source")
  }
}
