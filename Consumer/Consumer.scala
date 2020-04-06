import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Consumer extends App {
  import java.util.Properties

  val sparkConf = new SparkConf()
    .setAppName("Spark")
    .setMaster("local[*]")

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val TOPIC="Drone_info"

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "drone")


  val props1 = new Properties()
  props1.put("bootstrap.servers", "localhost:9092")
  props1.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props1.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props1)

  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Any]]
  }

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))


  while(true){
    val records=consumer.poll(500)
    println(records)

    records.asScala.foreach{r =>
      val df = jsonStrToMap(r.value())
      if(df("Alert") == 1){
        val record = new ProducerRecord[String, String]("alert_info", "key", r.value())
        val record1 = new ProducerRecord[String, String]("storage_info", "key", r.value())
        producer.send(record)
        producer.send(record1)
        println(df("Alert"))
      }else{
        val record = new ProducerRecord[String, String]("storage_info", "key", r.value())
        producer.send(record)
        println("Pas d'alerte")
      }
    }

  }

  producer.close()
  consumer.close()

}
