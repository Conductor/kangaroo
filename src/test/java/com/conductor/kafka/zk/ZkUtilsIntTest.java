package com.conductor.kafka.zk;

import com.conductor.kafka.Broker;
import com.conductor.kafka.IntTestBase;
import com.conductor.kafka.Partition;
import com.conductor.kafka.hadoop.KafkaInputFormat;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * Integration test reading the config written by kafka to zookeeper.
 *
 * Created by Greg Temchenko on 10/2/15.
 */
public class ZkUtilsIntTest extends IntTestBase {

    static protected ZkUtils zkUtils;

    @BeforeClass
    static public void setUpTest() {
        // the client that we test
        zkUtils = new ZkUtils("localhost:" + zkPort,
                KafkaInputFormat.DEFAULT_ZK_ROOT,
                KafkaInputFormat.DEFAULT_ZK_SESSION_TIMEOUT_MS,
                KafkaInputFormat.DEFAULT_ZK_CONNECTION_TIMEOUT_MS);
    }

    @Test
    public void getBrokers() {
//        SimpleConsumer consumer = new SimpleConsumer("localhost", kafkaPort, 100000, 64 * 1024, "testClientId");

        // check getBrokers()
        List<Broker> brokerList = zkUtils.getBrokers();
        assert brokerList.size() > 0;
        assert brokerList.get(0).getHost().equals("localhost");
        assert brokerList.get(0).getPort() == kafkaPort;

        // check getBroker()
        Integer id = brokerList.get(0).getId();
        Broker broker = zkUtils.getBroker(id);
        assert brokerList.get(0).getHost().equals(broker.getHost());
        assert brokerList.get(0).getPort() == broker.getPort();
        assert brokerList.get(0).getId() == id;
    }

    @Test
    public void getPartitions() {
        // check getPartitions()
        List<Partition> partitions = zkUtils.getPartitions(TEST_TOPIC);

        assert partitions.size() > 0;
        Partition part = partitions.get(0);
        assert part.getPartId() == 0;
        assert part.getTopic().equals(TEST_TOPIC);
        assert part.getBroker().getHost().equals("localhost");
        assert part.getBroker().getPort() == kafkaPort;

        // check partitionExists()
        assert zkUtils.partitionExists(TEST_TOPIC, part.getPartId());
        assert ! zkUtils.partitionExists(TEST_TOPIC, 99999);
    }

    @Test
    public void commit() {
        List<Partition> partitions = zkUtils.getPartitions(TEST_TOPIC);
        Partition part = partitions.get(0);

        // check getLastCommit()
        Long lastCommit = zkUtils.getLastCommit(TEST_GROUP, part);
        assert lastCommit == -1L;

        // check temporary setLastCommit(), not commited so getLastCommit() == -1L
        zkUtils.setLastCommit(TEST_GROUP, part, 10, true);
        lastCommit = zkUtils.getLastCommit(TEST_GROUP, part);
        assert lastCommit == -1L;

        // commit the change
        zkUtils.commit(TEST_GROUP, TEST_TOPIC);

        // check committed
        lastCommit = zkUtils.getLastCommit(TEST_GROUP, part);
        assert lastCommit == 10L;

        // check not temporary setLastCommit()
        zkUtils.setLastCommit(TEST_GROUP, part, 20, false);
        lastCommit = zkUtils.getLastCommit(TEST_GROUP, part);
        assert lastCommit == 20L;
    }

}
