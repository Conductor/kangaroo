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

package com.conductor.kafka.hadoop;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.conductor.kafka.Broker;
import com.conductor.kafka.Partition;
import com.conductor.kafka.zk.ZkUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;

/**
 * An {@link InputFormat} that splits up Kafka {@link Broker}-{@link Partition}s further into a set of offsets.
 * 
 * <p>
 * Specifically, it will call {@link SimpleConsumer#getOffsetsBefore} to retrieve a list of valid offsets, and create
 * {@code N} number of {@link InputSplit}s per {@link Broker}-{@link Partition}, where {@code N} is the number of
 * offsets returned by {@link SimpleConsumer#getOffsetsBefore}.
 * 
 * <p>
 * Thanks to <a href="https://github.com/miniway">Dongmin Yu</a> for providing the inspiration for this code.
 * 
 * <p>
 * The original source code can be found <a target="_blank" href="https://github.com/miniway/kafka-hadoop-consumer">on
 * Github</a>.
 * 
 * @see KafkaInputSplit
 * @see KafkaRecordReader
 * 
 * @author <a href="mailto:cgreen@conductor.com">Casey Green</a>
 */
public class KafkaInputFormat extends InputFormat<LongWritable, BytesWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInputFormat.class);

    /**
     * Default Kafka fetch size, 1MB.
     */
    public static final int DEFAULT_FETCH_SIZE_BYTES = 1024 * 1024; // 1MB
    /**
     * Default Kafka socket timeout, 10 seconds.
     */
    public static final int DEFAULT_SOCKET_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(10);
    /**
     * Default Kafka buffer size, 64KB.
     */
    public static final int DEFAULT_BUFFER_SIZE_BYTES = 64 * 1024; // 64 KB
    /**
     * Default Zookeeper session timeout, 10 seconds.
     */
    public static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(10);
    /**
     * Default Zookeeper connection timeout, 10 seconds.
     */
    public static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(10);
    /**
     * Default Zookeeper root, '/'.
     */
    public static final String DEFAULT_ZK_ROOT = "/";
    /**
     * Default maximum number of partitions per split.
     */
    public static final int DEFAULT_MAX_SPLITS_PER_PARTITION = Integer.MAX_VALUE;
    /**
     * Default timestamp to include
     */
    public static final long DEFAULT_INCLUDE_OFFSETS_AFTER_TIMESTAMP = 0;

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new KafkaRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final String topic = getTopic(conf);
        final String group = getConsumerGroup(conf);
        return getInputSplits(conf, topic, group);
    }

    /**
     * Returns the {@code topic} splits of the consumer {@code group} that would be input to a {@link Job} configured
     * with the provided {@code conf}
     * <p>
     * This information may be useful for calculating the number of reducers your job will need.
     * <p>
     * <em>Note:</em> At the very least, {@code kafka.zk.connect} must be set in {@code conf}.
     *
     * @param conf
     *            the conf, containing at least the {@code kafka.zk.connect} setting.
     * @param topic
     *            the kafka topic of hypothetical job.
     * @param group
     *            the consumer group of the hypothetical job
     * @return the number of splits the hypothetical job would get.
     * @throws IOException IO error
     */
    public static List<InputSplit> getSplits(final Configuration conf, final String topic, final String group)
            throws IOException {
        return new KafkaInputFormat().getInputSplits(conf, topic, group);
    }

    /**
     * Returns all of the {@code topic} splits that would be input to a {@link Job} configured with the provided
     * {@code conf}.
     * <p>
     * This information may be useful for calculating the number of reducers your job will need.
     * <p>
     * <em>Note:</em> At the very least, {@code kafka.zk.connect} must be set in {@code conf}.
     *
     * @param conf
     *            the conf, containing at least the {@code kafka.zk.connect} setting.
     * @param topic
     *            the kafka topic of hypothetical job.
     * @return the number of splits the hypothetical job would get.
     * @throws IOException IO error
     */
    public static List<InputSplit> getAllSplits(final Configuration conf, final String topic) throws IOException {
        // use a random UUID as the consumer group to (basically) guarantee a non-existent consumer
        return new KafkaInputFormat().getInputSplits(conf, topic, UUID.randomUUID().toString());
    }

    /**
     * Gets all of the input splits for the {@code topic}, filtering out any {@link InputSplit}s already consumed by the
     * {@code group}.
     * 
     * @param conf
     *            the job configuration.
     * @param topic
     *            the topic.
     * @param group
     *            the consumer group.
     * @return input splits for the job.
     * @throws IOException
     */
    List<InputSplit> getInputSplits(final Configuration conf, final String topic, final String group)
            throws IOException {
        final List<InputSplit> splits = Lists.newArrayList();
        final ZkUtils zk = getZk(conf);
        final Map<Broker, SimpleConsumer> consumers = Maps.newHashMap();
        try {
            for (final Partition partition : zk.getPartitions(topic)) {

                // cache the consumer connections - each partition will make use of each broker consumer
                final Broker broker = partition.getBroker();
                if (!consumers.containsKey(broker)) {
                    consumers.put(broker, getConsumer(broker, conf));
                }

                // grab all valid offsets
                final List<Long> offsets = getOffsets(consumers.get(broker), topic, partition.getPartId(),
                        zk.getLastCommit(group, partition), getIncludeOffsetsAfterTimestamp(conf),
                        getMaxSplitsPerPartition(conf), conf);
                for (int i = 0; i < offsets.size() - 1; i++) {
                    // ( offsets in descending order )
                    final long start = offsets.get(i + 1);
                    final long end = offsets.get(i);
                    // since the offsets are in descending order, the first offset in the list is the largest offset for
                    // the current partition. This split will be in charge of committing the offset for this partition.
                    final boolean partitionCommitter = (i == 0);
                    final InputSplit split = new KafkaInputSplit(partition, start, end, partitionCommitter);
                    LOG.debug("Created input split: " + split);
                    splits.add(split);
                }
            }
        } finally {
            // close resources
            IOUtils.closeQuietly(zk);
            for (final SimpleConsumer consumer : consumers.values()) {
                consumer.close();
            }
        }
        return splits;
    }

    @VisibleForTesting
    List<Long> getOffsets(final SimpleConsumer consumer, final String topic, final int partitionNum,
            final long lastCommit, final long asOfTime, final int maxSplitsPerPartition, final Configuration conf) {
        // TODO: take advantage of new API, which allows you to request offsets for multiple topic-partitions.

        // all offsets that exist for this partition (in descending order)
        final OffsetRequest allReq = toOffsetRequest(topic, partitionNum, kafka.api.OffsetRequest.LatestTime(),
                Integer.MAX_VALUE, conf);
        final OffsetResponse allOffsetsResponse = consumer.getOffsetsBefore(allReq);
        final long[] allOffsets = allOffsetsResponse.offsets(topic, partitionNum);

        // this gets us an offset that is strictly before 'asOfTime', or zero if none exist before that time
        final OffsetRequest requestBeforeAsOf = toOffsetRequest(topic, partitionNum, asOfTime, 1, conf);
        final OffsetResponse offsetsBeforeAsOfResponse = consumer.getOffsetsBefore(requestBeforeAsOf);
        final long[] offsetsBeforeAsOf = offsetsBeforeAsOfResponse.offsets(topic, partitionNum);
        final long includeAfter = offsetsBeforeAsOf.length == 1 ? offsetsBeforeAsOf[0] : -1;

        // note that the offsets are in descending order
        List<Long> result = Lists.newArrayList();
        LOG.debug("Offsets returned by SimpleConsumer: " + Arrays.toString(allOffsets));
        for (final long offset : allOffsets) {
            if (offset > lastCommit && offset > includeAfter) {
                result.add(offset);
            } else if (lastCommit == -1L && offset > includeAfter) {
                // nothing commited yet, so consume everything
                result.add(offset);
            } else {
                // we add "lastCommit" if it is after "includeAfter"
                if (lastCommit > includeAfter) {
                    result.add(lastCommit +1);
                }
                // we can break out of loop here bc offsets are in desc order, and we've hit the latest one to include
                break;
            }
        }
        // to get maxSplitsPerPartition number of splits, you need (maxSplitsPerPartition + 1) number of offsets.
        if (result.size() - 1 > maxSplitsPerPartition) {
            result = result.subList(result.size() - maxSplitsPerPartition - 1, result.size());
        }
        LOG.debug(String.format("Offsets for %s:%d:%d = %s", consumer.host(), consumer.port(), partitionNum, result));
        return result;
    }

    @VisibleForTesting
    static OffsetRequest toOffsetRequest(final String topic, final int partitionNum, final long asOfTime,
            final int numOffsets, final Configuration conf) {
        final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionNum);
        final PartitionOffsetRequestInfo partitionInfoReq = new PartitionOffsetRequestInfo(asOfTime, numOffsets);
        final Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = ImmutableMap.of(topicAndPartition,
                partitionInfoReq);
        return new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                KafkaInputFormat.getConsumerGroup(conf));
    }

    /*
     * We make the following two methods visible for testing so that we can mock these components out in unit tests
     */

    @VisibleForTesting
    SimpleConsumer getConsumer(final Broker broker, final Configuration conf) {
        return new SimpleConsumer(broker.getHost(), broker.getPort(), DEFAULT_SOCKET_TIMEOUT_MS,
                DEFAULT_BUFFER_SIZE_BYTES, KafkaInputFormat.getConsumerGroup(conf));
    }

    @VisibleForTesting
    ZkUtils getZk(final Configuration conf) {
        return new ZkUtils(conf);
    }

    /**
     * Sets the Zookeeper connection string (required).
     * 
     * @param job
     *            the job being configured
     * @param zkConnect
     *            zookeeper connection string.
     */
    public static void setZkConnect(final Job job, final String zkConnect) {
        job.getConfiguration().set("kafka.zk.connect", zkConnect);
    }

    /**
     * Gets the Zookeeper connection string set by {@link #setZkConnect(Job, String)}.
     * 
     * @param conf
     *            the job conf.
     * @return the Zookeeper connection string.
     */
    public static String getZkConnect(final Configuration conf) {
        return conf.get("kafka.zk.connect");
    }

    /**
     * Set the Zookeeper session timeout for Kafka.
     * 
     * @param job
     *            the job being configured.
     * @param sessionTimeout
     *            the session timeout in milliseconds.
     */
    public static void setZkSessionTimeoutMs(final Job job, final int sessionTimeout) {
        job.getConfiguration().setInt("kafka.zk.session.timeout.ms", sessionTimeout);
    }

    /**
     * Gets the Zookeeper session timeout set by {@link #setZkSessionTimeoutMs(Job, int)}, defaulting to
     * {@link #DEFAULT_ZK_SESSION_TIMEOUT_MS} if it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Zookeeper session timeout.
     */
    public static int getZkSessionTimeoutMs(final Configuration conf) {
        return conf.getInt("kafka.zk.session.timeout.ms", DEFAULT_ZK_SESSION_TIMEOUT_MS);
    }

    /**
     * Set the Zookeeper connection timeout for Zookeeper.
     * 
     * @param job
     *            the job being configured.
     * @param connectionTimeout
     *            the connection timeout in milliseconds.
     */
    public static void setZkConnectionTimeoutMs(final Job job, final int connectionTimeout) {
        job.getConfiguration().setInt("kafka.zk.connection.timeout.ms", connectionTimeout);
    }

    /**
     * Gets the Zookeeper connection timeout set by {@link #setZkConnectionTimeoutMs(Job, int)}, defaulting to
     * {@link #DEFAULT_ZK_CONNECTION_TIMEOUT_MS} if it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Zookeeper connection timeout.
     */
    public static int getZkConnectionTimeoutMs(final Configuration conf) {
        return conf.getInt("kafka.zk.connection.timeout.ms", DEFAULT_ZK_CONNECTION_TIMEOUT_MS);
    }

    /**
     * Sets the Zookeeper root for Kafka.
     * 
     * @param job
     *            the job being configured.
     * @param root
     *            the zookeeper root path.
     */
    public static void setZkRoot(final Job job, final String root) {
        job.getConfiguration().set("kafka.zk.root", root);
    }

    /**
     * Gets the Zookeeper root of Kafka set by {@link #setZkRoot(Job, String)}, defaulting to {@link #DEFAULT_ZK_ROOT}
     * if it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Zookeeper root of Kafka.
     */
    public static String getZkRoot(final Configuration conf) {
        return conf.get("kafka.zk.root", DEFAULT_ZK_ROOT);
    }

    /**
     * Sets the input topic (required).
     * 
     * @param job
     *            the job being configured
     * @param topic
     *            the topic name
     */
    public static void setTopic(final Job job, final String topic) {
        job.getConfiguration().set("kafka.topic", topic);
    }

    /**
     * Gets the input topic.
     * 
     * @param conf
     *            the job conf.
     * @return the input topic.
     */
    public static String getTopic(final Configuration conf) {
        return conf.get("kafka.topic");
    }

    /**
     * Sets the consumer group of the input reader (required).
     * 
     * @param job
     *            the job being configured.
     * @param consumerGroup
     *            consumer group name.
     */
    public static void setConsumerGroup(final Job job, final String consumerGroup) {
        job.getConfiguration().set("kafka.groupid", consumerGroup);
    }

    /**
     * Gets the consumer group.
     * 
     * @param conf
     *            the job conf.
     * @return the consumer group.
     */
    public static String getConsumerGroup(final Configuration conf) {
        return conf.get("kafka.groupid");
    }

    /**
     * Only consider partitions created <em>approximately</em> on or after {@code timestamp}.
     * <p>
     * Note that you are only guaranteed to get all data on or after {@code timestamp}, but you may get <i>some</i> data
     * before the specified timestamp.
     * 
     * @param job
     *            the job being configured.
     * @param timestamp
     *            the timestamp.
     * @see SimpleConsumer#getOffsetsBefore
     */
    public static void setIncludeOffsetsAfterTimestamp(final Job job, final long timestamp) {
        job.getConfiguration().setLong("kafka.timestamp.offset", timestamp);
    }

    /**
     * Gets the offset timestamp set by {@link #setIncludeOffsetsAfterTimestamp(Job, long)}, returning {@code 0} by
     * default.
     * 
     * @param conf
     *            the job conf.
     * @return the offset timestamp, {@code 0} by default.
     */
    public static long getIncludeOffsetsAfterTimestamp(final Configuration conf) {
        return conf.getLong("kafka.timestamp.offset", DEFAULT_INCLUDE_OFFSETS_AFTER_TIMESTAMP);
    }

    /**
     * Limits the number of splits to create per partition.
     * <p>
     * Note that it if there more partitions to consume than {@code maxSplits}, the input format will take the
     * <em>earliest</em> Kafka partitions.
     * 
     * @param job
     *            the job to configure.
     * @param maxSplits
     *            the maximum number of splits to create from each Kafka partition.
     */
    public static void setMaxSplitsPerPartition(final Job job, final int maxSplits) {
        job.getConfiguration().setInt("kafka.max.splits.per.partition", maxSplits);
    }

    /**
     * Gets the maximum number of splits per partition set by {@link #setMaxSplitsPerPartition(Job, int)}, returning
     * {@link Integer#MAX_VALUE} by default.
     * 
     * @param conf
     *            the job conf
     * @return the maximum number of splits, {@link Integer#MAX_VALUE} by default.
     */
    public static int getMaxSplitsPerPartition(final Configuration conf) {
        return conf.getInt("kafka.max.splits.per.partition", DEFAULT_MAX_SPLITS_PER_PARTITION);
    }

    /**
     * Sets the fetch size of the {@link RecordReader}. Note that your mapper should have enough memory allocation to
     * handle the specified size, or else you will likely throw {@link OutOfMemoryError}s.
     * 
     * @param job
     *            the job being configured.
     * @param fetchSize
     *            the fetch size (bytes).
     */
    public static void setKafkaFetchSizeBytes(final Job job, final int fetchSize) {
        job.getConfiguration().setInt("kafka.fetch.size", fetchSize);
    }

    /**
     * Gets the Kafka fetch size set by {@link #setKafkaFetchSizeBytes(Job, int)}, defaulting to
     * {@link #DEFAULT_FETCH_SIZE_BYTES} if it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Kafka fetch size.
     */
    public static int getKafkaFetchSizeBytes(final Configuration conf) {
        return conf.getInt("kafka.fetch.size", DEFAULT_FETCH_SIZE_BYTES);
    }

    /**
     * Sets the buffer size of the {@link SimpleConsumer} inside of the {@link KafkaRecordReader}.
     * 
     * @param job
     *            the job being configured.
     * @param bufferSize
     *            the buffer size (bytes).
     */
    public static void setKafkaBufferSizeBytes(final Job job, final int bufferSize) {
        job.getConfiguration().setInt("kafka.socket.buffersize", bufferSize);
    }

    /**
     * Gets the Kafka buffer size set by {@link #setKafkaBufferSizeBytes(Job, int)}, defaulting to
     * {@link #DEFAULT_BUFFER_SIZE_BYTES} if it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Kafka buffer size.
     */
    public static int getKafkaBufferSizeBytes(final Configuration conf) {
        return conf.getInt("kafka.socket.buffersize", DEFAULT_BUFFER_SIZE_BYTES);
    }

    /**
     * Sets the socket timeout of the {@link SimpleConsumer} inside of the {@link KafkaRecordReader}.
     * 
     * @param job
     *            the job being configured.
     * @param timeout
     *            the socket timeout (milliseconds).
     */
    public static void setKafkaSocketTimeoutMs(final Job job, final int timeout) {
        job.getConfiguration().setInt("kafka.socket.timeout.ms", timeout);
    }

    /**
     * Gets the Kafka socket timeout set by {@link #setKafkaSocketTimeoutMs(Job, int)}, defaulting to
     * {@link #DEFAULT_SOCKET_TIMEOUT_MS} if it has not been set.
     * 
     * @param conf
     *            the job conf.
     * @return the Kafka socket timeout.
     */
    public static int getKafkaSocketTimeoutMs(final Configuration conf) {
        return conf.getInt("kafka.socket.timeout.ms", DEFAULT_SOCKET_TIMEOUT_MS);
    }
}
