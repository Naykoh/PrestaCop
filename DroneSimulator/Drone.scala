import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.kafka.clients.producer._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

case class Drone_(`_c0`: Int,
                  `Summons Number`: Long,
                  `Plate ID`: String,
                  `Registration State`: String,
                  `Plate Type`: String,
                  `Issue Date`: String,
                  `Violation Code`:Int,
                  `Vehicle Body Type` : String,
                  `Vehicle Make` : String,
                  `Vehicle Expiration Date` : Int,
                  `Violation Time` : String,
                  `Vehicle Color` : String,
                  `Vehicle Year` : Double,
                  `Latitude` : Double,
                  `Longitude` : Double,
                  `Alert` : Int,
                  `ImageID` : String,
                  `DroneID` : String
)


object Drone{
  def main (args: Array[String]): Unit = {


    val pathToFile = "data/nyc-parking-ticket-2014.csv"


    val sparkConf = new SparkConf()
      .setAppName("Spark")
      .setMaster("local[*]")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()


    val df = sparkSession
      .read.
      format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(pathToFile)



    val df1 = df.where(df("DroneID") === "Drone1")
    val df2 = df.where(df("DroneID") === "Drone2")
    val df3 = df.where(df("DroneID") === "Drone3")
    val df4 = df.where(df("DroneID") === "Drone4")


    import sparkSession.implicits._


    val drone1 = df1.as[Drone_]
    val drone2 = df2.as[Drone_]
    val drone3 = df3.as[Drone_]
    val drone4 = df4.as[Drone_]


    val drone1_json_format = drone1.toJSON.collect()
    val drone2_json_format = drone2.toJSON.collect()
    val drone3_json_format = drone3.toJSON.collect()
    val drone4_json_format = drone4.toJSON.collect()



    val TOPIC="Drone_info"

    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val t1 = Future[Unit] {
      drone1_json_format.foreach{ row =>
        val record = new ProducerRecord(TOPIC, "key", row)
        producer.send(record)
        Thread.sleep(2000)
      }
    }

    val t2 = Future[Unit] {
      drone2_json_format.foreach{ row =>
        val record = new ProducerRecord(TOPIC, "key", row)
        producer.send(record)
        Thread.sleep(2000)
      }
    }

    val t3 = Future[Unit] {
      drone3_json_format.foreach{ row =>
        val record = new ProducerRecord(TOPIC, "key", row)
        producer.send(record)
        Thread.sleep(2000)
      }
    }

    val t4 = Future[Unit] {
      drone4_json_format.foreach{ row =>
        val record = new ProducerRecord(TOPIC, "key", row)
        producer.send(record)
        Thread.sleep(500)
      }
    }

    val futures = Future.sequence(Seq[Future[Unit]](t1, t2, t3, t4))

    futures.onSuccess {
      case value => println("All the csv lines has been streamed")
    }

    Await.ready(futures,Duration.Inf)

    producer.close()

  }



}
