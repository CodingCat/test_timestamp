package org.apache.spark.streaming.eventhubs

import com.microsoft.azure.eventhubs.EventData

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestStampWithSpark {

  def main(args: Array[String]): Unit = {


    val eventHubsParameters = Map[String, String](
      "eventhubs.namespace" -> "xxxxxxxxxxxxxx",
      "eventhubs.name" -> "xxxxxxxxxxxxxx",
      "eventhubs.policyname" -> "xxxxxxxxxxxxxx",
      "eventhubs.policykey" -> "xxxxxxxxxxxxxx",
      "eventhubs.consumergroup" -> "xxxxxxxxxxxxxx",
      "eventhubs.partition.count" -> "xxxxxxxxxxxxxx",
      "eventhubs.checkpoint.interval" -> "xxxxxxxxxxxxxx",
      "eventhubs.checkpoint.dir" -> "xxxxxxxxxxxxxx",
      "eventhubs.filter.enqueuetime" -> s"${args(0)}"
    )


    val sparkConfiguration = new SparkConf()

    sparkConfiguration.setAppName(this.getClass.getSimpleName)
    sparkConfiguration.set("spark.streaming.driver.writeAheadLog.allowBatching", "true")
    sparkConfiguration.set("spark.streaming.driver.writeAheadLog.batchingTimeout", "60000")
    sparkConfiguration.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    sparkConfiguration.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
    sparkConfiguration.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
    sparkConfiguration.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val streamingContext = new StreamingContext(new SparkContext(sparkConfiguration), Seconds(5))
    streamingContext.sparkContext.setLogLevel(args(2))
    streamingContext.checkpoint(args(1))

    val eventHubsStream = EventHubsUtils.createUnionStream(streamingContext, eventHubsParameters)

    eventHubsStream.map(binaryData => new String(binaryData)).print(100)

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
