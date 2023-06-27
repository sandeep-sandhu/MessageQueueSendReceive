/**
  * file: MessageQueueSendReceive.scala

  * Purpose: Implements an application
  * that pushes events from a csv file to a Kafka message queue
  * and reads from the file too.

  * References:
  *
  *
  *
  * cd MessageQueueSendReceive
  * scala MessageQueueSendReceive application.conf
  *
  *
  */

package io.github.sandeep_sandhu.stream_analytics

import java.io.File
import java.util.Properties
import java.nio.ByteBuffer
import java.sql.Timestamp

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.log4j.{Level, Logger}

import com.typesafe.config.{Config, ConfigFactory}
import scala.annotation.tailrec
import scala.io.Source
import scala.jdk.CollectionConverters._

object MessageQueueSendReceive {
  val appName: String = "MessageQueueSendReceive";
  val logger: Logger = Logger.getLogger(appName)
  val conf: Config = ConfigFactory.load()
  val settings: AppSettings = new AppSettings(conf)
  var topic: String = "topic1";
  var broker: String = "localhost:9092";
  var inputFileName: String = "file.csv";

  val usage =
    """
Usage: MessageQueueSendReceive [--mode send|receive]
"""

  def main(args: Array[String]): Unit = {

    logger.setLevel(Level.INFO)

    logger.info(s"Started the application: $appName.")
    if (args.length == 0) {
      println(usage)
      System.exit(1)
    }

    val cmdArgOptions = nextArg(Map(), args.toList)

    if (args.length > 1) {
      if (cmdArgOptions("mode").equalsIgnoreCase("send")) {
        this.inputFileName = settings.input_filename
        this.topic = settings.topic
        this.broker = settings.broker
        sendMessages();
      } else {
        receiveMessages()
      }
    }

  }

  def sendMessages(): Unit = {

    val producer = configProducer(this.topic, this.broker)
    this.logger.info(s"Started the producer: $producer.")

    try {
      val bufferedSource = Source.fromFile(this.inputFileName)

      for (line <- bufferedSource.getLines()) {
        // Assuming each line in the CSV file represents a message to be sent to Kafka
        val fields: Array[String] = line.split(',')
        // TODO: parse and fill fields into record
        val customRecord = TransactionRecord(
          new Timestamp(System.currentTimeMillis()),
          1234L,
          45.67,
          1
        )
        this.SendRecord(producer, customRecord)
      }

      bufferedSource.close()
    } catch {
      case e: Exception => {
        this.logger.error(s"Error caught when sending messages: $e.")
        e.printStackTrace()
      }
    } finally {
      producer.close()
    }
  }

  def receiveMessages() = {
    val consumer = configConsumer(this.topic, this.broker)
    this.logger.info(s"Started the consumer: $consumer.")

    try {
      receive(consumer)
    } catch {
      case e: Exception => {
        this.logger.error(s"Error caught when receiving messages: $e.")
        e.printStackTrace()
      }
    } finally {
      consumer.close()
    }
  }

  /**
    * Parse the next command line argument, recursively building up the map.
    * @param map The hashmap in which all switches and their values are stored as key-value pairs
    * @param list The command line arguments passed as a list
    * @return
    */
  @tailrec
  final def nextArg(
    map: Map[String, String],
    list: List[String]
  ): Map[String, String] =
    list match {
      case Nil => map
      case "--mode" :: value :: tail =>
        nextArg(map ++ Map("mode" -> value.toLowerCase()), tail)
      case "--conf" :: value :: tail =>
        nextArg(map ++ Map("conf" -> value.toLowerCase()), tail)
      case unknown :: _ =>
        println("Unknown option " + unknown)
        map
    }

  def configProducer(
    topic: String,
    bootstrapServers: String
  ): KafkaProducer[String, Array[Byte]] = {

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[ByteArraySerializer].getName)

    new KafkaProducer[String, Array[Byte]](props)
  }

  def SendRecord(
    producer: KafkaProducer[String, Array[Byte]],
    customRecord: TransactionRecord
  ): Unit =
    try {
      val valueBytes = serializeTransactionRecord(customRecord)

      val serializedRecord = new ProducerRecord[String, Array[Byte]](
        this.topic,
        customRecord.ID.toString,
        valueBytes
      )

      producer.send(serializedRecord)

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      producer.close()
    }

  def serializeTransactionRecord(record: TransactionRecord): Array[Byte] = {

    val buffer = ByteBuffer.allocate(
      java.lang.Long.BYTES + java.lang.Long.BYTES + java.lang.Double.BYTES + java.lang.Integer.BYTES
    )

    buffer.putLong(record.eventTime.getTime)
    buffer.putLong(record.ID)
    buffer.putDouble(record.Amount)
    buffer.putInt(record.TxnType)

    buffer.array()
  }

  def configConsumer(
    topic: String,
    bootstrapServers: String
  ): KafkaConsumer[String, Array[Byte]] = {

    val groupId = "group1"

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      classOf[StringDeserializer].getName
    )
    props.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      classOf[ByteArrayDeserializer].getName
    )
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    new KafkaConsumer[String, Array[Byte]](props)
  }

  def receive(consumer: KafkaConsumer[String, Array[Byte]]): Unit = {

    consumer.subscribe(Seq(topic).asJava)

    try {
      while (true) {
        val records = consumer.poll(java.time.Duration.ofMillis(500))
        for (record <- records.asScala) {
          val valueBytes = record.value()
          val customRecord = deserializeTransactionRecord(valueBytes)
          println(s"Received record: $customRecord")
        }
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      consumer.close()
    }
  }

  def deserializeTransactionRecord(bytes: Array[Byte]): TransactionRecord = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val eventTime = new java.sql.Timestamp(buffer.getLong)
    val id = buffer.getLong
    val amount = buffer.getDouble
    val txnType = buffer.getInt
    TransactionRecord(eventTime, id, amount, txnType)
  }

}

class AppSettings(config: Config) extends Serializable {

  val logger: Logger = Logger.getLogger(MessageQueueSendReceive.appName)
  logger.setLevel(Level.INFO)

  logger.info("Started loading configuration from the config file.")

  val input_filename: String = config.getString("input_filename")
  val broker: String = config.getString("broker")
  val topic: String = config.getString("topic")
  val consumer_group_id: String = config.getString("consumer_group_id")

//  val outputFile: String = config.getString("file.output")
//  val jdbcDriver: String = config.getString("jdbc.driver")
//  val jdbcUser: String = config.getString("jdbc.user")
//  val jdbcPassword: String = config.getString("jdbc.password")
//  val jdbcUrl: String = config.getString("jdbc.url")
//  val jdbcTableName: String = config.getString("jdbc.tablename")
//  val minPartitions: Int = config.getInt("minPartitions")
//  val labelCol: String = config.getString("label.column")
//  val numericalFeatures: Array[String] =
//    config.getStringList("numerical.features").asScala.map(x => x).toArray
//  val categoricalFeatures: Array[String] =
//    config.getStringList("categorical.features").asScala.map(x => x).toArray
//  val lrModelSavePath: String = config.getString("lr.model.save.path")
//  val gbdtModelSavePath: String = config.getString("gbdt.model.save.path")
//  val transformPipelineSavePath: String =
//    config.getString("transform.pipeline.path")

}

case class TransactionRecord(
  eventTime: Timestamp,
  ID: Long,
  Amount: Double,
  TxnType: Int
);
