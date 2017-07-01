package org.test.pcap4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MainApp {

    private final static Logger log = LoggerFactory.getLogger(MainApp.class);

    public static void main(String... args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MainApp.class.getName());
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Minutes.apply(1L));

        JavaReceiverInputDStream<byte[]> dStream = jsc.receiverStream(new CaptureNetworkReceiver());
        dStream.foreachRDD((VoidFunction<JavaRDD<byte[]>>) packetJavaRDD -> {
            String s = packetJavaRDD.rdd().toDebugString();
            System.out.println(s);
        });

        jsc.start();
        jsc.awaitTermination();
    }

}

