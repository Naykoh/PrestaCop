import org.apache.spark.{SparkConf, SparkContext, rdd}
import


object Reader extends App{


  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._


  val sparkConf = new SparkConf()
    .setAppName("Spark")
    .setMaster("local[]")

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("AppName")
      .config("spark.master", "local")
      .getOrCreate()

  val df_load:DataFrame = spark.read.text("hdfs://localhost:9000/storage-data/parking-data2.txt")
  import spark.implicits._


  df_load.printSchema()


  val schema = new StructType()
    .add("_c0", StringType, true)
    .add("Summons Number", StringType, true)
    .add("Plate ID", StringType, true)
    .add("Registration State", StringType, true)
    .add("Plate Type", StringType, true)
    .add("Issue Date", StringType, true)
    .add("Violation Code", StringType, true)
    .add("Vehicle Body Type", StringType, true)
    .add("Vehicle Make", StringType, true)
    .add("Vehicle Expiration Date", StringType, true)
    .add("Violation Time", StringType, true)
    .add("Vehicle Color", StringType, true)
    .add("Vehicle Year", StringType, true)
    .add("Latitude", StringType, true)
    .add("Longitude", StringType, true)
    .add("Alert", StringType, true)
    .add("DroneID", StringType, true)

  val dfJSON = df_load.withColumn("jsonData",from_json(col("value"),schema))
    .select("jsonData.")
  dfJSON.printSchema()
  dfJSON.show(false)

}
