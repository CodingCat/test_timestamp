package org.apache.spark.streaming.eventhubs

import java.net.URI
import java.time.Instant

import com.microsoft.azure.eventhubs.{EventHubClient, PartitionReceiver}
import com.microsoft.azure.servicebus.ConnectionStringBuilder

import org.apache.spark.streaming.eventhubs.EventhubsOffsetType.EventhubsOffsetType
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestTimeStamp {


  private var eventhubsReceiver: PartitionReceiver = _
  private val MINIMUM_PREFETCH_COUNT: Int = 10
  private var MAXIMUM_PREFETCH_COUNT: Int = 999
  private var MAXIMUM_EVENT_RATE: Int = 0
  private val DEFAULT_RECEIVER_EPOCH: Long = -1

  private def buildReceiver(eventhubsParams: Map[String, String],
                            partitionId: String,
                            maximumEventRate: Int): Unit = {
    //Create Eventhubs connection string either from namespace (with default URI) or from specified URI

    if(eventhubsParams.contains("eventhubs.uri") && eventhubsParams.contains("eventhubs.namespace")) {

      throw new IllegalArgumentException(s"Eventhubs URI and namespace cannot both be specified at the same time.")
    }

    val connectionString = if(eventhubsParams.contains("eventhubs.namespace"))
      new ConnectionStringBuilder(eventhubsParams("eventhubs.namespace"),
        eventhubsParams("eventhubs.name"), eventhubsParams("eventhubs.policyname"),
        eventhubsParams("eventhubs.policykey"))
    else if (eventhubsParams.contains("eventhubs.uri"))
      new ConnectionStringBuilder(new URI(eventhubsParams("eventhubs.uri")),
        eventhubsParams("eventhubs.name"), eventhubsParams("eventhubs.policyname"),
        eventhubsParams("eventhubs.policykey"))
    else
      throw new IllegalArgumentException(s"Either Eventhubs URI or namespace nust be specified.")

    //Set the consumer group if specified.

    val consumerGroup: String = if( eventhubsParams.contains("eventhubs.consumergroup"))
      eventhubsParams("eventhubs.consumergroup")
    else EventHubClient.DEFAULT_CONSUMER_GROUP_NAME

    //Set the epoch if specified

    val receiverEpoch: Long = 0

    //Determine the offset to start receiving data

    var offsetType = EventhubsOffsetType.None
    var currentOffset: String = PartitionReceiver.START_OF_STREAM

    val previousOffset = "-1"

    if(previousOffset != "-1" && previousOffset != null) {

      offsetType = EventhubsOffsetType.PreviousCheckpoint
      currentOffset = previousOffset

    } else if (eventhubsParams.contains("eventhubs.filter.offset")) {

      offsetType = EventhubsOffsetType.InputByteOffset
      currentOffset = eventhubsParams("eventhubs.filter.offset")

    } else if (eventhubsParams.contains("eventhubs.filter.enqueuetime")) {

      offsetType = EventhubsOffsetType.InputTimeOffset
      currentOffset = eventhubsParams("eventhubs.filter.enqueuetime")
    }

    MAXIMUM_EVENT_RATE = maximumEventRate

    if (maximumEventRate > 0 && maximumEventRate < MINIMUM_PREFETCH_COUNT)
      MAXIMUM_PREFETCH_COUNT = MINIMUM_PREFETCH_COUNT
    else if (maximumEventRate >= MINIMUM_PREFETCH_COUNT && maximumEventRate < MAXIMUM_PREFETCH_COUNT)
      MAXIMUM_PREFETCH_COUNT = MAXIMUM_EVENT_RATE + 1
    else MAXIMUM_EVENT_RATE = MAXIMUM_PREFETCH_COUNT - 1

    createReceiverInternal(connectionString.toString, consumerGroup, partitionId, offsetType,
      currentOffset, receiverEpoch)
  }

  private[eventhubs]
  def createReceiverInternal(connectionString: String,
                             consumerGroup: String,
                             partitionId: String,
                             offsetType: EventhubsOffsetType,
                             currentOffset: String,
                             receiverEpoch: Long): Unit = {

    //Create Eventhubs client
    val eventhubsClient: EventHubClient = EventHubClient.createFromConnectionStringSync(connectionString)

    //Create Eventhubs receiver  based on the offset type and specification
    offsetType match  {

      case EventhubsOffsetType.None => eventhubsReceiver = if(receiverEpoch > DEFAULT_RECEIVER_EPOCH)
        eventhubsClient.createEpochReceiverSync(consumerGroup, partitionId, currentOffset, receiverEpoch)
      else eventhubsClient.createReceiverSync(consumerGroup, partitionId, currentOffset)

      case EventhubsOffsetType.PreviousCheckpoint => eventhubsReceiver = if(receiverEpoch > DEFAULT_RECEIVER_EPOCH)
        eventhubsClient.createEpochReceiverSync(consumerGroup, partitionId, currentOffset, false, receiverEpoch)
      else  eventhubsClient.createReceiverSync(consumerGroup, partitionId, currentOffset, false)

      case EventhubsOffsetType.InputByteOffset => eventhubsReceiver = if(receiverEpoch > DEFAULT_RECEIVER_EPOCH)
        eventhubsClient.createEpochReceiverSync(consumerGroup, partitionId, currentOffset, false, receiverEpoch)
      else eventhubsClient.createReceiverSync(consumerGroup, partitionId, currentOffset, false)

      case EventhubsOffsetType.InputTimeOffset => eventhubsReceiver = if(receiverEpoch > DEFAULT_RECEIVER_EPOCH)
        eventhubsClient.createEpochReceiverSync(consumerGroup, partitionId, Instant.ofEpochSecond(currentOffset.toLong),
          receiverEpoch)
      else eventhubsClient.createReceiverSync(consumerGroup, partitionId, Instant.ofEpochSecond(currentOffset.toLong))
    }

    eventhubsReceiver.setPrefetchCount(MAXIMUM_PREFETCH_COUNT)
  }


  def main(args: Array[String]): Unit = {

    import scala.collection.JavaConverters._

    val eventHubsParameters = Map[String, String](
      "eventhubs.namespace" -> "xxxxxxxxxxxxx",
      "eventhubs.name" -> "xxxxxxxxxxxxxx",
      "eventhubs.policyname" -> "xxxxxxxxxx2",
      "eventhubs.policykey" -> "xxxxxxxxxxxxxx",
      "eventhubs.consumergroup" -> "xxxxxxxxxxxxxx",
      "eventhubs.partition.count" -> "xxxxxxxxxxxxxx",
      "eventhubs.checkpoint.interval" -> "xxxxxxxxxxxxxx",
      "eventhubs.checkpoint.dir" -> "xxxxxxxxxxxxxx"
    ) ++ {if (args.length > 2) Map("eventhubs.filter.enqueuetime" -> s"${args(2)}") else Map()}

    buildReceiver(eventHubsParameters, args(1), 900)
    for (i <- 0 until args(0).toInt) {
      val l = eventhubsReceiver.receive(10).get().asScala.toList
      l.foreach(eventhub => print(eventhub.getSystemProperties.getEnqueuedTime.getEpochSecond +
        "\t" + new String(eventhub.getBody) + "\t"))
    }
    println()
  }
}
