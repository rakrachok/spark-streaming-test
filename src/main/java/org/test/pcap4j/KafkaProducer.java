package org.test.pcap4j;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KafkaProducer extends org.apache.kafka.clients.producer.KafkaProducer<String, String>
        implements Serializable {

    private static final ConcurrentMap<String, Boolean> nifAlerts = new ConcurrentHashMap<>();

    private static final String MESSAGE_TOPIC = "alerts";
    public static final Map<String, Object> PRODUCER_PROPERTIES = new HashMap<>();

    {
        String brokerList = System.getProperty("kafka.brokerList");

        PRODUCER_PROPERTIES.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        PRODUCER_PROPERTIES.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        PRODUCER_PROPERTIES.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
    }

    public KafkaProducer() {
        super(PRODUCER_PROPERTIES);
    }

    public void send(String nifName, boolean isAlert, String data) {
        Boolean curFlag = nifAlerts.get(nifName);
        if (curFlag == null || isAlert != curFlag) {
            nifAlerts.put(nifName, isAlert);
            ProducerRecord<String, String> record = new ProducerRecord<>(MESSAGE_TOPIC, null, data);
            send(record);
        }
    }

}
