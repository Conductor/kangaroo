package com.conductor.kafka;

import info.batey.kafka.unit.KafkaUnit;
import kafka.producer.KeyedMessage;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Integration test base class. Includes Kafka and Zookeeper instances
 *
 * Created by Greg Temchenko on 10/5/15.
 */
public class IntTestBase {

    final static protected String TEST_TOPIC = "test-topic";
    final static protected String TEST_GROUP = "test-group";
    final static protected int NUMBER_EVENTS = 100;

    protected static KafkaUnit kafka = null;

    static protected int kafkaPort;
    static protected int zkPort;

    @BeforeClass
    static public void setUp() {
        // embedded kafka + zookeeper, thus we
        zkPort = 11111;
        kafkaPort = 11112; // TODO pick dynamically so can run in parallel on the same machine
        kafka = new KafkaUnit(zkPort, kafkaPort);

        // use small segments to test multiple segments with smaller amount of records
        kafka.setKafkaBrokerConfig("log.segment.bytes", "1024");

        kafka.startup();

        // create new topic and put some test messages
        kafka.createTopic(TEST_TOPIC);

        for (int i = 1; i <= NUMBER_EVENTS; i++) {
            KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(TEST_TOPIC,
                    "test-key-" + Integer.valueOf(i), getMessageBody(i));
            kafka.sendMessages(keyedMessage);
        }
    }

    @AfterClass
    static public void tearDown() throws Exception {
        if (kafka != null) {
            kafka.shutdown();
        }
    }

    static protected String getMessageBody(Integer messageNumber) {
        return "test-message-" + String.format("%05d", messageNumber);
    }

}
