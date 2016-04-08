package com.conductor.kafka.hadoop;

import com.conductor.kafka.IntTestBase;

import com.conductor.kafka.zk.ZkUtils;
import kafka.producer.KeyedMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by Greg Temchenko on 10/6/15.
 */
public class KafkaRecordReaderIntTest extends IntTestBase {

    KafkaInputFormat kafkaInputFormat;
    Configuration conf;

    @Test
    public void readingTest() throws Exception {
        // config
        kafkaInputFormat = new KafkaInputFormat();

        conf = new Configuration();
        conf.set("kafka.zk.connect", "localhost:" + Integer.valueOf(zkPort));
        conf.set("kafka.topic", TEST_TOPIC);
        conf.set("kafka.groupid", TEST_GROUP);

        JobConf jobConf = new JobConf(conf);
        JobContext jobContext = new JobContextImpl(jobConf, JobID.forName("job_test_123"));
        List<InputSplit> splits = kafkaInputFormat.getSplits(jobContext);

        assert splits.size() > 0 : "No input splits generated";
        assert splits.size() > 1 : "There should be more than one splits to test it appropriately";

        // read every split
        List<String> readEvents = readSplitEvents(splits);

        assertEquals(NUMBER_EVENTS, readEvents.size());

        // Messages returns in order within a partition. So we sort all read messages
        // and check there are no dups or unexpected events
        verifyMessages(readEvents, 0, NUMBER_EVENTS);

        // SECOND PART
        // Commit offsets, generate more records, check it consumes only the new records

        // commit
        ZkUtils zkUtils = new ZkUtils(conf);
        zkUtils.commit(TEST_GROUP, TEST_TOPIC);

        // generate more records
        final int NUMBER_NEW_EVENTS = 150;
        for (int i = NUMBER_EVENTS +1; i <= NUMBER_EVENTS + NUMBER_NEW_EVENTS; i++) {
            KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(TEST_TOPIC,
                    "test-key-" + Integer.valueOf(i), getMessageBody(i));
            kafka.sendMessages(keyedMessage);
        }

        // there might be a delay in log flush, so we wait for a bit
        Thread.sleep(500);

        // check it consumes only the new records
        List<InputSplit> splits2 = kafkaInputFormat.getSplits(jobContext);
        assert splits2.size() > 1;

        List<String> readEvents2 = readSplitEvents(splits2);
        Collections.sort(readEvents2);
        assertEquals(NUMBER_NEW_EVENTS, readEvents2.size());
        verifyMessages(readEvents2, NUMBER_EVENTS, NUMBER_EVENTS + NUMBER_EVENTS);
    }

    private List<String> readSplitEvents(List<InputSplit> splits) throws Exception {
        List<String> records = new ArrayList<>();
        for (InputSplit split : splits) {
            TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, TaskAttemptID.forName("attempt_test_123_m12_34_56"));

            RecordReader recordReader = kafkaInputFormat.createRecordReader(split, attemptContext);
            recordReader.initialize(split, attemptContext);

            // read values
            while (recordReader.nextKeyValue()) {
                assert recordReader.getCurrentKey() != null;
                assert recordReader.getCurrentValue() != null;
                assert recordReader.getCurrentValue() instanceof BytesWritable;

                BytesWritable bytesWritable = (BytesWritable) recordReader.getCurrentValue();
                String kafkaEvent = new String(
                        java.util.Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength()), StandardCharsets.UTF_8);

                records.add(kafkaEvent);
            }
            recordReader.close();
        }
        return records;
    }

    private void verifyMessages(List<String> readEvents, int firstEvent, int lastEvent) {
        // Messages returns in order within a partition. So we sort all read messages
        // and check there are no dups or unexpected events
        Collections.sort(readEvents);
        for (int i=0; i<lastEvent-firstEvent; i++) {
            assertEquals("Incorrect message. Maybe there are duplicates or unexpected events?",
                    getMessageBody(firstEvent+i+1), readEvents.get(i));
        }
    }

}