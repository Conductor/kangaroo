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

import static com.conductor.kafka.hadoop.KafkaInputFormat.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import kafka.api.*;
import kafka.common.ErrorMapping;
import kafka.consumer.SimpleConsumer;
import kafka.message.*;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.Iterator;

import com.conductor.kafka.zk.ZkUtils;
import com.google.common.annotations.VisibleForTesting;

/**
 * A record reader that reads a subsection, [{@link #getStart()}, {@link #getEnd()}), of a Kafka queue
 * {@link com.conductor.kafka.Partition}.
 * 
 * <p>
 * Thanks to <a href="https://github.com/miniway">Dongmin Yu</a> for providing the inspiration for this code.
 * 
 * <p>
 * The original source code can be found <a target="_blank" href="https://github.com/miniway/kafka-hadoop-consumer">on
 * Github</a>.
 * 
 * @see KafkaInputSplit
 * @see KafkaInputFormat
 * @see MultipleKafkaInputFormat
 * 
 * @author <a href="mailto:cgreen@conductor.com">Casey Green</a>
 */
public class KafkaRecordReader extends RecordReader<LongWritable, BytesWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaInputFormat.class);

    private Configuration conf;
    private KafkaInputSplit split;
    private SimpleConsumer consumer;
    private Iterator<MessageAndOffset> currentMessageItr;
    private LongWritable key;
    private BytesWritable value;
    private long start;
    private long end;
    private long pos;
    private int fetchSize;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException,
            InterruptedException {
        if (!(split instanceof KafkaInputSplit)) {
            throw new IllegalArgumentException("Expected an InputSplit of type KafkaInputSplit but got "
                    + split.getClass());
        }

        final KafkaInputSplit inputSplit = (KafkaInputSplit) split;
        this.conf = context.getConfiguration();
        this.split = inputSplit;
        this.start = inputSplit.getStartOffset();
        this.pos = inputSplit.getStartOffset();
        this.end = inputSplit.getEndOffset();
        this.fetchSize = KafkaInputFormat.getKafkaFetchSizeBytes(conf);
        this.consumer = getConsumer(inputSplit, conf);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        consumer.close();
        if (split.isPartitionCommitter()) {
            commitOffset();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (pos >= end || start == end) {
            return 1.0f;
        }
        return Math.min(1.0f, (pos - start) / (float) (end - start));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new BytesWritable();
        }
        if (continueItr()) {
            final MessageAndOffset msg = getCurrentMessageItr().next();
            final long msgOffset = msg.offset();
            final Message message = msg.message();
            final ByteBuffer buffer = message.payload();
            value.set(buffer.array(), buffer.arrayOffset(), message.payloadSize());
            key.set(msgOffset);
            pos = msgOffset;
            return true;
        }
        return false;
    }

    /**
     * THIS METHOD HAS SIDE EFFECTS - it will update {@code currentMessageItr} (if necessary) and then return true iff
     * the iterator still has elements to be read. If you call {@link scala.collection.Iterator#next()} when this method
     * returns false, you risk a {@link NullPointerException} OR a no-more-elements exception.
     * 
     * @return true if you can call {@link scala.collection.Iterator#next()} on {@code currentMessageItr}.
     */
    @VisibleForTesting
    boolean continueItr() {
        final long remaining = end - pos -1; // we exclude the last element. A split 10-20 means elements from 10 to 19.
        if (!canCallNext() && remaining > 0) {
            LOG.debug(String.format("%s fetching %d bytes starting at offset %d",
                    split.toString(), fetchSize, pos));
            final String topic = split.getPartition().getTopic();
            final int partition = split.getPartition().getPartId();
            final FetchRequest request = new FetchRequestBuilder()
                    .addFetch(topic, partition, pos, fetchSize)
                    .clientId(KafkaInputFormat.getConsumerGroup(conf))
                    .build();
            final FetchResponse fetchResponse = consumer.fetch(request);
            if (fetchResponse.hasError()) {
                final short code = fetchResponse.errorCode(topic, partition);
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    return false;
                }
                ErrorMapping.maybeThrowException(code);
            }

            final ByteBufferMessageSet byteBufferMessageSet = fetchResponse.messageSet(topic, partition);
            currentMessageItr = byteBufferMessageSet.iterator();
        }
        return canCallNext();
    }

    @VisibleForTesting
    boolean canCallNext() {
        return getCurrentMessageItr() != null && getCurrentMessageItr().hasNext();
    }

    @VisibleForTesting
    void commitOffset() throws IOException {
        ZkUtils zk = null;
        try {
            zk = getZk();
            /**
             * Note: last parameter (temp) MUST be true. It is up to the ToolRunner to commit offsets upon successful
             * job execution. Reason: since there are multiple input splits per partition, the consumer group could get
             * into a bad state if this split finished successfully and committed the offset while another input split
             * from the same partition didn't finish successfully.
             */
            zk.setLastCommit(getConsumerGroup(conf), split.getPartition(), pos, true);
        } finally {
            IOUtils.closeQuietly(zk);
        }
    }

    /*
     * We make the following methods visible for testing so that we can mock these components out in unit tests
     */

    @VisibleForTesting
    SimpleConsumer getConsumer(final KafkaInputSplit split, final Configuration conf) {
        return new SimpleConsumer(split.getPartition().getBroker().getHost(), split.getPartition().getBroker()
                .getPort(), getKafkaSocketTimeoutMs(conf), getKafkaBufferSizeBytes(conf),
                KafkaInputFormat.getConsumerGroup(conf));
    }

    @VisibleForTesting
    ZkUtils getZk() {
        return new ZkUtils(conf);
    }

    public Configuration getConf() {
        return conf;
    }

    public KafkaInputSplit getSplit() {
        return split;
    }

    public Iterator<MessageAndOffset> getCurrentMessageItr() {
        return currentMessageItr;
    }

    public long getStart() {
        return start;
    }

    public long getPos() {
        return pos;
    }

    public long getEnd() {
        return end;
    }

    public int getFetchSize() {
        return fetchSize;
    }

}
