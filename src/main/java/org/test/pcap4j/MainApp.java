package org.test.pcap4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class MainApp {

    private final static Logger log = LoggerFactory.getLogger(MainApp.class);

    private final static int UPPER_LIMIT = 1073741824;

    public static void main(String... args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MainApp.class.getName());
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Minutes.apply(1L));

        JavaReceiverInputDStream<Tuple2<String, byte[]>> dStream = jsc.receiverStream(new CaptureNetworkReceiver());
        JavaPairDStream<String, Integer> pairDStream = dStream
            .mapToPair((PairFunction<Tuple2<String, byte[]>, String, Integer>) t -> new Tuple2<>(t._1, t._2.length));

        pairDStream.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>) pairRDD -> {
            JavaPairRDD<String, Integer> sizePerNif = pairRDD
                .reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);
            sizePerNif.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
                if (tuple._2 > UPPER_LIMIT) {
                    log.warn("Alert: the amount of data suppressed the limit");
                }
            });
        });

        jsc.start();
        jsc.awaitTermination();
    }

}

