import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.kafka.clients.producer._
import org.joda.time.DateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

case class Drone_(
                   `_c0`: Int,
                   `Summons Number`: Long,
                   `Plate ID`: String,
                   `Registration State`: String,
                   `Plate Type`: String,
                   `Issue Date`: String,
                   `Violation Code`: Int,
                   `Vehicle Body Type` : String,
                   `Vehicle Make` : String,
                   `Vehicle Expiration Date` : Int,
                   `Violation Time` : String,
                   `Vehicle Color` : String,
                   `Vehicle Year` : Double,
                   `Latitude` : Double,
                   `Longitude` : Double,
                   `Alert` : Int,
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
    /*val df5 = df.where(df("DroneID") === "Drone5")
    val df6 = df.where(df("DroneID") === "Drone6")
    val df7 = df.where(df("DroneID") === "Drone7")
    val df8 = df.where(df("DroneID") === "Drone8")
    val df9 = df.where(df("DroneID") === "Drone9")
    val df10 = df.where(df("DroneID") === "Drone10")*/

    //df1.show(10)


    import sparkSession.implicits._


    val drone1 = df1.as[Drone_]
    val drone2 = df2.as[Drone_]
    val drone3 = df3.as[Drone_]
    val drone4 = df4.as[Drone_]
    /*val drone5 = df5.as[Drone_]
    val drone6 = df6.as[Drone_]
    val drone7 = df7.as[Drone_]
    val drone8 = df8.as[Drone_]
    val drone9 = df9.as[Drone_]
    val drone10 = df10.as[Drone_]*/


    val drone1_json_format = drone1.toJSON.collect()
    val drone2_json_format = drone2.toJSON.collect()
    val drone3_json_format = drone3.toJSON.collect()
    val drone4_json_format = drone4.toJSON.collect()
    /*val drone5_json_format = drone5.toJSON.collect()
    val drone6_json_format = drone6.toJSON.collect()
    val drone7_json_format = drone7.toJSON.collect()
    val drone8_json_format = drone8.toJSON.collect()
    val drone9_json_format = drone9.toJSON.collect()
    val drone10_json_format = drone10.toJSON.collect()*/


    println(drone1.getClass(),drone1_json_format.getClass( ))

    val df_json_format = df.toJSON.collect()

    drone1_json_format.foreach{ row =>
      println(row)
    }

    val TOPIC="drone1"

    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](props)

    val t1 = Future[Unit] {
      drone1_json_format.foreach{ row =>
        val record = new ProducerRecord(TOPIC, "key", row)
        producer.send(record)
        Thread.sleep(5000)
      }
    }

    val t2 = Future[Unit] {
      drone2_json_format.foreach{ row =>
        val record = new ProducerRecord(TOPIC, "key", row)
        producer.send(record)
        Thread.sleep(5000)
      }
    }

    val t3 = Future[Unit] {
      drone3_json_format.foreach{ row =>
        val record = new ProducerRecord(TOPIC, "key", row)
        producer.send(record)
        Thread.sleep(5000)
      }
    }

    val t4 = Future[Unit] {
      drone4_json_format.foreach{ row =>
        val record = new ProducerRecord(TOPIC, "key", row)
        producer.send(record)
        Thread.sleep(5000)
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
