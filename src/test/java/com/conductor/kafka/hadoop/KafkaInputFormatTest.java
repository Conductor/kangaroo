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

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;

import kafka.api.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;

import com.conductor.kafka.Broker;
import com.conductor.kafka.Partition;
import com.conductor.kafka.zk.ZkUtils;
import com.google.common.collect.Lists;

/**
 * @author cgreen
 */
public class KafkaInputFormatTest {

    @Test
    public void testSetGet() throws Exception {
        final Configuration conf = new Configuration(false);
        final Job mockJob = mock(Job.class);
        when(mockJob.getConfiguration()).thenReturn(conf);

        KafkaInputFormat.setZkConnect(mockJob, "zk-1:8021");
        assertEquals("zk-1:8021", KafkaInputFormat.getZkConnect(conf));

        KafkaInputFormat.setZkSessionTimeoutMs(mockJob, 100);
        assertEquals(100, KafkaInputFormat.getZkSessionTimeoutMs(conf));

        KafkaInputFormat.setZkConnectionTimeoutMs(mockJob, 101);
        assertEquals(101, KafkaInputFormat.getZkConnectionTimeoutMs(conf));

        KafkaInputFormat.setZkRoot(mockJob, "/prod");
        assertEquals("/prod", KafkaInputFormat.getZkRoot(conf));

        KafkaInputFormat.setTopic(mockJob, "my_topic");
        assertEquals("my_topic", KafkaInputFormat.getTopic(conf));

        KafkaInputFormat.setConsumerGroup(mockJob, "consumer_group");
        assertEquals("consumer_group", KafkaInputFormat.getConsumerGroup(conf));

        KafkaInputFormat.setIncludeOffsetsAfterTimestamp(mockJob, 111l);
        assertEquals(111l, KafkaInputFormat.getIncludeOffsetsAfterTimestamp(conf));

        KafkaInputFormat.setMaxSplitsPerPartition(mockJob, 2);
        assertEquals(2, KafkaInputFormat.getMaxSplitsPerPartition(conf));

        KafkaInputFormat.setKafkaFetchSizeBytes(mockJob, 88);
        assertEquals(88, KafkaInputFormat.getKafkaFetchSizeBytes(conf));

        KafkaInputFormat.setKafkaBufferSizeBytes(mockJob, 77);
        assertEquals(77, KafkaInputFormat.getKafkaBufferSizeBytes(conf));

        KafkaInputFormat.setKafkaSocketTimeoutMs(mockJob, 655);
        assertEquals(655, KafkaInputFormat.getKafkaSocketTimeoutMs(conf));
    }

    @Test
    public void testGetInputSplits() throws Exception {
        final KafkaInputFormat inputFormat = spy(new KafkaInputFormat());
        final SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
        final ZkUtils mockZk = mock(ZkUtils.class);
        final Configuration mockConf = new Configuration(false);

        final Broker broker = new Broker("127.0.0.1", 9092, 1);
        doReturn(mockConsumer).when(inputFormat).getConsumer(broker, mockConf);
        doReturn(mockZk).when(inputFormat).getZk(mockConf);
        doReturn(Lists.newArrayList(20l, 10l)).when(inputFormat).getOffsets(mockConsumer, "topic", 0, -1, 0,
                Integer.MAX_VALUE, mockConf);
        doReturn(Lists.newArrayList(30l, 20l, 0l)).when(inputFormat).getOffsets(mockConsumer, "topic", 1, 10, 0,
                Integer.MAX_VALUE, mockConf);

        final Partition p1 = new Partition("topic", 0, broker);
        final Partition p2 = new Partition("topic", 1, broker);
        when(mockZk.getPartitions("topic")).thenReturn(Lists.newArrayList(p1, p2));
        when(mockZk.getBroker(1)).thenReturn(broker);
        when(mockZk.getLastCommit("group", p1)).thenReturn(-1l);
        when(mockZk.getLastCommit("group", p2)).thenReturn(10l);

        final List<InputSplit> result = inputFormat.getInputSplits(mockConf, "topic", "group");

        // assert the contents of each split
        Assert.assertEquals(3, result.size());
        final KafkaInputSplit split1 = (KafkaInputSplit) result.get(0);
        final Broker broker1 = split1.getPartition().getBroker();
        assertEquals(broker, broker1);
        assertEquals("127.0.0.1", broker1.getHost());
        assertEquals(9092, broker1.getPort());
        assertEquals(1, broker1.getId());
        assertEquals("1-0", split1.getPartition().getBrokerPartition());
        assertEquals(0, split1.getPartition().getPartId());
        assertEquals(10l, split1.getStartOffset());
        assertEquals(20l, split1.getEndOffset());
        assertEquals("topic", split1.getPartition().getTopic());

        final KafkaInputSplit split2 = (KafkaInputSplit) result.get(1);
        assertEquals(20l, split2.getStartOffset());
        assertEquals(30l, split2.getEndOffset());
        assertEquals("1-1", split2.getPartition().getBrokerPartition());

        final KafkaInputSplit split3 = (KafkaInputSplit) result.get(2);
        assertEquals(0l, split3.getStartOffset());
        assertEquals(20l, split3.getEndOffset());
        assertEquals("1-1", split3.getPartition().getBrokerPartition());

        // verify one and only one call to getConsumer - should get the cached consumer second time around
        verify(inputFormat, times(1)).getConsumer(broker, mockConf);
        verify(inputFormat, times(1)).getConsumer(any(Broker.class), any(Configuration.class));

        // verify the closeable components are closed
        verify(mockConsumer, times(1)).close();
        verify(mockZk, times(1)).close();
    }

    @Test
    public void testGetOffsets() throws Exception {
        final SimpleConsumer consumer = mock(SimpleConsumer.class);
        final Configuration mockConf = new Configuration(false);

        final OffsetResponse responseAll = mock(OffsetResponse.class);
        final long[] offsets = { 101, 91, 81, 71, 61, 51, 41, 31, 21, 11 };
        when(responseAll.offsets("topic", 1)).thenReturn(offsets);

        final kafka.javaapi.OffsetRequest offsetRequestAll = KafkaInputFormat.toOffsetRequest("topic", 1,
                OffsetRequest.LatestTime(), Integer.MAX_VALUE, mockConf);
        when(consumer.getOffsetsBefore(offsetRequestAll)).thenReturn(responseAll);

        final OffsetResponse responseAsOf = mock(OffsetResponse.class);
        when(responseAsOf.offsets("topic", 1)).thenReturn(new long[] {});
        final kafka.javaapi.OffsetRequest offsetRequestAsOf = KafkaInputFormat.toOffsetRequest("topic", 1, 0, 1, mockConf);
        when(consumer.getOffsetsBefore(offsetRequestAsOf)).thenReturn(responseAsOf);

        final KafkaInputFormat inputFormat = new KafkaInputFormat();

        // case 0: get everything (-1 last commit, 0 asOfTime, as many partitions as possible) -> all offsets
        long[] expected = offsets;
        List<Long> actual = inputFormat.getOffsets(consumer, "topic", 1, -1, 0, Integer.MAX_VALUE, mockConf);
        compareArrayContents(offsets, actual);

        // case 1: lastCommit of 52 -> we should only get back the first 5 offsets + the lastCommit
        final int lastCommit = 52;
        expected = new long[6];
        System.arraycopy(offsets, 0, expected, 0, 6);
        expected[5] = lastCommit +1;
        actual = inputFormat.getOffsets(consumer, "topic", 1, lastCommit, 0, Integer.MAX_VALUE, mockConf);
        compareArrayContents(expected, actual);

        // case 2: lastCommit of 52, asOfTime 51 -> still include last offsets
        final int asOfTime = 999;
        final kafka.javaapi.OffsetRequest requestAsOf999 = KafkaInputFormat.toOffsetRequest("topic", 1, asOfTime, 1,
                mockConf);
        when(responseAsOf.offsets("topic", 1)).thenReturn(new long[] { 51 });
        when(consumer.getOffsetsBefore(requestAsOf999)).thenReturn(responseAsOf);
        actual = inputFormat.getOffsets(consumer, "topic", 1, lastCommit, asOfTime, Integer.MAX_VALUE, mockConf);
        compareArrayContents(expected, actual);

        // case 3: lastCommit of 52, asOfTime 52 -> don't include last offsets
        when(responseAsOf.offsets("topic", 1)).thenReturn(new long[] { 52 });
        when(consumer.getOffsetsBefore(requestAsOf999)).thenReturn(responseAsOf);
        expected = Arrays.copyOfRange(offsets, 0, 5);
        actual = inputFormat.getOffsets(consumer, "topic", 1, lastCommit, asOfTime, Integer.MAX_VALUE, mockConf);
        compareArrayContents(expected, actual);

        // case 4: maxSplitsPerPartition == number of commits (5) -> should include all 5 offsets
        actual = inputFormat.getOffsets(consumer, "topic", 1, lastCommit, asOfTime, 5, mockConf);
        compareArrayContents(expected, actual);

        // case 5: maxSplitsPerPartition = number of commits - 1 (4) -> should STILL include all 5 offsets
        actual = inputFormat.getOffsets(consumer, "topic", 1, lastCommit, asOfTime, 4, mockConf);
        compareArrayContents(expected, actual);

        // case 6: maxSplitsPerPartition = number of commits - 2 (3) -> should exclude the first (largest) offset
        actual = inputFormat.getOffsets(consumer, "topic", 1, lastCommit, asOfTime, 3, mockConf);
        expected = Arrays.copyOfRange(offsets, 1, 5);
        compareArrayContents(expected, actual);

        // case 7: maxSplitsPerPartition = 1 -> should include just 2 commits
        actual = inputFormat.getOffsets(consumer, "topic", 1, lastCommit, asOfTime, 1, mockConf);
        expected = Arrays.copyOfRange(offsets, 3, 5);
        compareArrayContents(expected, actual);
    }

    private void compareArrayContents(final long[] expected, final List<Long> actual) {
        Assert.assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; ++i) {
            Assert.assertEquals(format("Expected %d but got %d at index %d", expected[i], actual.get(i), i),
                    expected[i], (long) actual.get(i));
        }
    }
}
