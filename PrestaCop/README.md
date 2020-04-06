Big-Data-Project

Project done by BENAISSA Aymen, HAMANI Nayel, NOWACKI Gregoire and LAHLOU Mamoun
/*
The goal was to provide a simple Lambda architecture using Kafka, Spark, Spark Streaming and HDFS like system to store datas that have been simulated by a Python script.

Files:

    dataSimulation: Generate JSON File to simulate IOT Devices
    myProducer: Producer reading the JSON and sending JSON format messages to Kafka (beeData topic)
    scalaDataReader: Spark code reading files in the HDFakeSystem and performing simple analysis
    zeppelin: Spark Streaming module consumming datas in the Kafka's topic and creating realtime graphs
    streamingConsumer: Spark Streaming module consumming datas in the Kafka's topic and sending alerts if certain levels such as low fuel devices are reached
    bashConsumer: Spark streaming consumming every x mn in order to exctract datas from Kafka and put them as csv in the HDFakeSystem
*/
