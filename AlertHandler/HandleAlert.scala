import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import java.util
import javax.mail.internet.InternetAddress
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods._


object HandleAlert extends App {

  import java.util.Properties

  val sparkConf = new SparkConf()
    .setAppName("Spark")
    .setMaster("local[*]")A

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val TOPIC = "alert_info"

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "drone")



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

      val hour = df("Violation Time").toString.slice(0,2)
      val min = df("Violation Time").toString.slice(2,4)
      val stateDay = df("Violation Time").toString.slice(4,5) + "M"


      val timeResult = hour + ":"+ min +" "+stateDay

      mailer(Envelope.from(nayel)
        .to(ryan)
        .subject("Alert #"+df("Summons Number"))
        .content(Multipart()
          .html("<html><body>" +
            "<img height='200' src='https://pngimage.net/wp-content/uploads/2018/06/nypd-logo-png-1.png'>"+
            "<h1>We have detected an alert from " + df("DroneID") +
            ": </h1> <br>" +
            "<h3>Violation code : "+df("Violation Code")+"<br>"+
            "Violation Time : "+ timeResult+"</h3><br>"+
            "<h2>Image of the violation below :</h2><br>"+
            "<img height='300' width='300' src='"+df("ImageID")+"'>"+"<br><br><br>"+
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
