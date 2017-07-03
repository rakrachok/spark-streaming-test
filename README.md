Spark Streaming app to monitor Network Interfaces
=================================================
This application is a test task for DINS.

It consists of 3 Java classes:
1) An entry point for launching streaming context and processing data received from the custom receiver.
2) A custom receiver to performing collecting NIF packets via Pcap API (Pcap4j). The each NIF is being processed in the separate thread.
3) A message produces to send messages to Kafka with alerts in case if the amount of traffic processed suppressed the allowed limit or reached the value less than the limit.

To build this project use

    mvn install

To run this project from within Maven use

    mvn exec:java
    
For the further information please contact me via email (`rakrachok@gmail.com`)