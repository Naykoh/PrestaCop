# PrestaCop

Project done by BENAISSA Aymen, HAMANI Nayel, NOWACKI Gregoire and LAHLOU Mamoun.

PrestaCop, a company specializing in service delivery for police forces, wants to create a drone service to help systems make parking tickets.
A camera with pattern recognition software identifies license plates and characterizes infractions.


Files:

  DroneSimulator : Generate JSON File to simulate drone data, and Producer reading thosz JSON file to Kafka into drone    
                 topic
  
  AlertHandler : Consumer that takes the alert topic and send a mail with location of the violation.
  
  Consumer : Spark streaming consuming drone topic and Producer that distinguish alert. Creating two others topics, one with only alert for the AlertHandler and one other topic to store into HDFS.
  
  FileReader : Read hdfs file, convert into DataFrame.
  
  flume.conf : 
