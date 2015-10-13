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
        // Embedded kafka + zookeeper
        zkPort = 11111;
        kafkaPort = 11112; // TODO pick dynamically so can run in parallel on the same machine
        kafka = new KafkaUnit(zkPort, kafkaPort);

        // Use small segments to test multiple segments with smaller amount of records
        kafka.setKafkaBrokerConfig("log.segment.bytes", "1024");

        // Flush data often so we don't have moments when we verify results that's not been flushed yet.
        // Makes performance bad, but reliable results.
        kafka.setKafkaBrokerConfig("log.flush.interval.messages", "1");
        kafka.setKafkaBrokerConfig("log.flush.scheduler.interval.ms", "100");
        kafka.setKafkaBrokerConfig("log.flush.interval.ms", "100");

        kafka.startup();

        // Create new topic and put some test messages
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
        // Zeros padding makes messages possible to sort just by its string values
        return "test-message-" + String.format("%05d", messageNumber);
    }

}
