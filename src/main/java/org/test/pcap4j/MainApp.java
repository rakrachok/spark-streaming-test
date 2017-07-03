package org.test.pcap4j;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;

public class MainApp {
    private final static Logger log = LoggerFactory.getLogger(MainApp.class);

    private final static Duration BATCH_DURATION = Minutes.apply(5L);
    private final static String WAREHOUSE_LOCATION = "spark-warehouse";

    public static void main(String... args) throws Exception {
        JavaSparkContext jc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());

        SparkSession ss = SparkSession
            .builder()
            .config("spark.sql.warehouse.dir", WAREHOUSE_LOCATION)
            .enableHiveSupport()
            .getOrCreate();

        JavaStreamingContext jsc = new JavaStreamingContext(jc, BATCH_DURATION);

        JavaReceiverInputDStream<Tuple2<String, byte[]>> dStream = jsc.receiverStream(new CaptureNetworkReceiver());
        JavaPairDStream<String, Integer> pairDStream = dStream
            .mapToPair((PairFunction<Tuple2<String, byte[]>, String, Integer>) t -> new Tuple2<>(t._1, t._2.length));

        Dataset<Row> ds = ss.sql("select limit_name, limit_value from traffic_limits" +
            " where limit_name in ('min', 'max') order by limit_value");
        Dataset<Row> limit = ds.limit(2);
        List<Row> rows = limit.toJavaRDD().collect();
//        int min = rows.get(0).getInt(1);
        int max = rows.get(1).getInt(1);

        pairDStream.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>) pairRDD -> {
            log.info("Batch's come from the receiver, size: " + pairRDD.count());
            KafkaProducer kafkaProducer = new KafkaProducer();
            JavaPairRDD<String, Integer> sizePerNif = pairRDD
                .reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);
            sizePerNif.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
                log.info("Host: " + tuple._1 + " Traffic passed: " + tuple._2);
                if (tuple._2 > max) {
                    kafkaProducer.send(tuple._1, true, "Alert: the amount of data suppressed the limit");
                } else {
                    kafkaProducer.send(tuple._1, false, "Info: the amount of data is under the limit");
                }
            });
        });

        jsc.start();
        jsc.awaitTermination();
    }
}

