import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import java.util

import javax.mail.internet.InternetAddress
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties


object HandleAlert extends App {



  val sparkConf = new SparkConf()
    .setAppName("Spark")
    .setMaster("local[*]")

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val TOPIC = "alert"

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "drone")


  val props1 = new Properties()
  props1.put("bootstrap.servers", "localhost:9092")
  props1.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props1.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Any]]
  }

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  import courier._, Defaults._
  val nayel = new InternetAddress("nayel.hamani@gmail.com")
  val ryan = new InternetAddress("aymen.benaissa@efrei.net")
  val mailer = Mailer("smtp.gmail.com", 587)
    .auth(true)
    .as("nayel.hamani@gmail.com", "gevbcmtkuhbmmkjv")
    .startTls(true)()


  while (true) {
    val records = consumer.poll(500)
    records.asScala.foreach { r =>
      val df = jsonStrToMap(r.value())
      println(df)

      val lon = df("Longitude")
      val lat = df("Latitude")

      mailer(Envelope.from(nayel)
        .to(nayel)
        .subject("Alert #"+df("Summons Number"))
        .content(Multipart()
            .html("<html><body>" +
              "<img height='200' src='https://pngimage.net/wp-content/uploads/2018/06/nypd-logo-png-1.png'>"+
              "<h1>We have detected an alert from " + df("DroneID") +
              " at : </h1> <br>" +
              "<img width='600' src='https://maps.googleapis.com/maps/api/staticmap?center="+ lat+","+lon + "&zoom=16&scale=1&size=600x300&maptype=roadmap&key=AIzaSyAMBfn2COKCUg6TtWYVkGOzX1b4vFLaqIA&format=png&visual_refresh=true&markers=size:mid%7Ccolor:0xff0000%7Clabel:%7C"+ lat+","+lon + "'>" +
              "<br>" +
              "<img height='200' src='https://pngimage.net/wp-content/uploads/2018/06/nypd-logo-png-1.png'>"+
              "</body></html>"
          )
        )
      ).onSuccess {
        case _ => println("message delivered")
      }
    }
  }
  consumer.close()
}