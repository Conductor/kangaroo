/**
 * Copyright 2014 Conductor, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * 
 */

package com.conductor.kafka.zk;

import static java.lang.String.format;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.hadoop.conf.Configuration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.conductor.kafka.Broker;
import com.conductor.kafka.Partition;
import com.conductor.kafka.hadoop.KafkaInputFormat;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Ranges;

/**
 * This class wraps some of the Kafka interactions with Zookeeper, namely {@link Broker} and {@link Partition} queries,
 * as well as consumer group offset operations and queries.
 * 
 * <p>
 * Thanks to <a href="https://github.com/miniway">Dongmin Yu</a> for providing the inspiration for this code.
 * 
 * <p>
 * The original source code can be found <a target="_blank" href="https://github.com/miniway/kafka-hadoop-consumer">on
 * Github</a>.
 * 
 * @author <a href="mailto:cgreen@conductor.com">Casey Green</a>
 */
public class ZkUtils implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

    private final ZkClient client;
    private final String zkRoot;

    @VisibleForTesting
    ZkUtils(final ZkClient client, final String zkRoot) {
        this.client = client;
        this.zkRoot = zkRoot.endsWith("/") ? zkRoot.substring(0, zkRoot.length() - 1) : zkRoot;
    }

    /**
     * Creates a Zookeeper client.
     * 
     * @param zkConnectionString
     *            the connection string for Zookeeper, e.g. {@code zk-1.com:2181,zk-2.com:2181}.
     * @param zkRoot
     *            the Zookeeper root of your Kafka configuration.
     * @param sessionTimeout
     *            Zookeeper session timeout for this client.
     * @param connectionTimeout
     *            Zookeeper connection timeout for this client.
     */
    public ZkUtils(final String zkConnectionString, final String zkRoot, final int sessionTimeout,
            final int connectionTimeout) {
        this(new ZkClient(zkConnectionString, sessionTimeout, connectionTimeout, new StringSerializer()), zkRoot);
    }

    public ZkUtils(final String zkConnectionString) {
        this(new ZkClient(
                        zkConnectionString,
                        KafkaInputFormat.DEFAULT_ZK_SESSION_TIMEOUT_MS,
                        KafkaInputFormat.DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                        new StringSerializer()),
                KafkaInputFormat.DEFAULT_ZK_ROOT);
    }

    /**
     * Creates a Zookeeper client based on the settings in {@link Configuration}.
     * 
     * @param config
     *            config with the Zookeeper settings in it.
     * @see KafkaInputFormat#getZkConnect(org.apache.hadoop.conf.Configuration)
     * @see KafkaInputFormat#getZkRoot(org.apache.hadoop.conf.Configuration)
     * @see KafkaInputFormat#getZkSessionTimeoutMs(org.apache.hadoop.conf.Configuration)
     * @see KafkaInputFormat#getZkConnectionTimeoutMs(org.apache.hadoop.conf.Configuration)
     */
    public ZkUtils(final Configuration config) {
        this(KafkaInputFormat.getZkConnect(config), // zookeeper connection string
                KafkaInputFormat.getZkRoot(config), // zookeeper root
                KafkaInputFormat.getZkSessionTimeoutMs(config), // session timeout
                KafkaInputFormat.getZkConnectionTimeoutMs(config)); // connection timeout
    }

    /**
     * Closes the Zookeeper client
     * 
     * @throws IOException IO error
     */
    @Override
    public void close() throws IOException {
        client.close();
    }

    /**
     * Gets the {@link Broker} by ID if it exists, {@code null} otherwise.
     * 
     * @param id
     *            the broker id.
     * @return a {@link Broker} if it exists, {@code null} otherwise.
     */
    public Broker getBroker(final Integer id) {
        String data = client.readData(getBrokerIdPath(id), true);
        if (!Strings.isNullOrEmpty(data)) {
            LOG.info("Broker " + id + " " + data);

            // parse json metadata
            JSONObject obj = new JSONObject(data);
            String hostname = obj.getString("host");
            Integer port = obj.getInt("port");

            return new Broker(hostname, port, id);
        }
        return null;
    }

    /**
     * Gets all of the {@link Broker}s in this Kafka cluster.
     * 
     * @return all of the {@link Broker}s in this Kafka cluster.
     */
    public List<Broker> getBrokers() {
        final List<Broker> brokers = Lists.newArrayList();
        final List<String> ids = getChildrenParentMayNotExist(getBrokerIdSubPath());
        for (final String id : ids) {
            brokers.add(getBroker(Integer.parseInt(id)));
        }
        return brokers;
    }

    /**
     * A {@link List} of all the {@link Partition} for a given {@code topic}.
     * 
     * @param topic
     *            the topic.
     * @return all the {@link Partition} for a given {@code topic}.
     */
    public List<Partition> getPartitions(final String topic) {
        final List<Partition> partitions = Lists.newArrayList();
        final List<String> parts = getChildrenParentMayNotExist(getTopicPartitions(topic));
        for (final String partitionId : parts) {
            final Integer pId = Integer.valueOf(partitionId);
            final String data = client.readData(getTopicPartitionState(topic, pId));

            // parse json metadata
            JSONObject obj = new JSONObject(data);
            Integer leader = obj.getInt("leader");

            final Broker broker = getBroker(leader);
            assert leader != null;
            partitions.add(new Partition(topic, pId, broker));
        }
        return partitions;
    }

    /**
     * Checks whether the provided partition exists on the {@link Broker}.
     *
     * @param topic
     *            the topic.
     * @param partId
     *            the partition id.
     * @return true if this partition exists on the {@link Broker}, false otherwise.
     */
    public boolean partitionExists(final String topic, final int partId) {
        final String parts = client.readData(getTopicPartitionState(topic, partId), true);
        return !Strings.isNullOrEmpty(parts);
    }

    /**
     * Gets the last commit made by the {@code group} on the {@code topic-partition}.
     * 
     * @param group
     *            the consumer group.
     * @param partition
     *            the partition.
     * @return the last offset, {@code -1} if the {@code group} has never committed an offset.
     */
    public long getLastCommit(String group, Partition partition) {
        final String offsetPath = getOffsetsPath(group, partition);
        final String offset = client.readData(offsetPath, true);
        LOG.debug("Offset path: '" + offsetPath + "' value: " + offset);

        if (offset == null) {
            return -1L;
        }
        return Long.valueOf(offset);
    }

    /**
     * ` Sets the last offset to {@code commit} of the {@code group} for the given {@code topic-partition}.
     * <p>
     * If {@code temp == true}, this will "temporarily" set the offset, in which case the user must call
     * {@link #commit(String, String)}. This is useful if a user wants to temporarily commit an offset for a topic
     * partition, and then commit it once <em>all</em> topic partitions have completed.
     * 
     * @param group
     *            the consumer group.
     * @param partition
     *            the partition.
     * @param commit
     *            the commit offset.
     * @param temp
     *            If {@code temp == true}, this will "temporarily" set the offset, in which case the user must call
     *            {@link #commit(String, String)}. This is useful if a user wants to temporarily commit an offset for a
     *            topic partition, and then commit it once the user has finished consuming <em>all</em> topic
     *            partitions.
     */
    public void setLastCommit(final String group, final Partition partition, final long commit, final boolean temp) {
        final String path = temp ? getTempOffsetsPath(group, partition) : getOffsetsPath(group, partition);
        if (!client.exists(path)) {
            client.createPersistent(path, true);
        }
        client.writeData(path, commit);
    }

    /**
     * Commits any temporary offsets of the {@code group} for a given {@code topic}.
     * 
     * @param group
     *            the consumer group.
     * @param topic
     *            the topic.
     * @return true if the commit was successful, false otherwise.
     */
    public boolean commit(final String group, final String topic) {
        for (final Partition partition : getPartitionsWithTempOffsets(topic, group)) {
            final String path = getTempOffsetsPath(group, partition);
            final String offset = client.readData(path);
            setLastCommit(group, partition, Long.valueOf(offset), false);
            client.delete(path);
        }
        return true;
    }

    private List<Partition> getPartitionsWithTempOffsets(final String topic, final String group) {
        final List<String> brokerPartIds = getChildrenParentMayNotExist(getTempOffsetsSubPath(group, topic));
        return Lists.transform(brokerPartIds, new Function<String, Partition>() {
            @Override
            public Partition apply(final String brokerPartId) {
                // brokerPartId = brokerId-partId
                final String[] brokerIdPartId = brokerPartId.split("-");
                final Broker broker = getBroker(Integer.parseInt(brokerIdPartId[0]));
                return new Partition(topic, Integer.parseInt(brokerIdPartId[1]), broker);
            }
        });
    }

    @VisibleForTesting
    List<String> getChildrenParentMayNotExist(String path) {
        try {
            return client.getChildren(path);
        } catch (final ZkNoNodeException e) {
            return Lists.newArrayList();
        }
    }

    @VisibleForTesting
    String getOffsetsPath(String group, Partition partition) {
        return format("%s/kangaroo-consumers/%s/offsets/%s/%s", zkRoot, group, partition.getTopic(),
                partition.getBrokerPartition());
    }

    @VisibleForTesting
    String getTempOffsetsPath(String group, Partition partition) {
        return format("%s/%s", getTempOffsetsSubPath(group, partition.getTopic()), partition.getBrokerPartition());
    }

    @VisibleForTesting
    String getTempOffsetsSubPath(String group, String topic) {
        return format("%s/kangaroo-consumers/%s/offsets-temp/%s", zkRoot, group, topic);
    }

    @VisibleForTesting
    String getBrokerIdSubPath() {
        return format("%s/brokers/ids", zkRoot);
    }

    @VisibleForTesting
    String getBrokerIdPath(final Integer id) {
        return format("%s/%d", getBrokerIdSubPath(), id);
    }

    @VisibleForTesting
    String getTopicPartitions(final String topic) {
        return format("%s/brokers/topics/%s/partitions", zkRoot, topic);
    }

    @VisibleForTesting
    String getTopicPartitionState(final String topic, final Integer partitionId) {
        return format("%s/%d/state", getTopicPartitions(topic), partitionId);
    }

    @VisibleForTesting
    static class StringSerializer implements ZkSerializer {

        public StringSerializer() {
        }

        @Override
        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null)
                return null;
            return new String(data);
        }

        @Override
        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }

    }

    @VisibleForTesting
    String getZkRoot() {
        return zkRoot;
    }
}
