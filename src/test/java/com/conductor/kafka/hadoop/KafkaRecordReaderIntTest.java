package com.conductor.kafka.hadoop;

import com.conductor.kafka.IntTestBase;

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

    @Test
    public void readingTest() throws Exception {
        // config
        KafkaInputFormat kafkaInputFormat = new KafkaInputFormat();

        Configuration conf = new Configuration();
        conf.set("kafka.zk.connect", "localhost:" + Integer.valueOf(zkPort));
        conf.set("kafka.topic", TEST_TOPIC);
        conf.set("kafka.groupid", TEST_GROUP);

        JobConf jobConf = new JobConf(conf);
        JobContext jobContext = new JobContextImpl(jobConf, JobID.forName("job_test_123"));
        List<InputSplit> splits = kafkaInputFormat.getSplits(jobContext);

        assert splits.size() > 0 : "No input splits generated";
        assert splits.size() > 1 : "There should be more than one splits to test it appropriately";

        // read every split
        List<String> readEvents = new ArrayList<>();
        for (InputSplit split : splits) {
            TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, TaskAttemptID.forName("attempt_test_123_m12_34_56"));

            RecordReader recordReader = kafkaInputFormat.createRecordReader(split, attemptContext);
            recordReader.initialize(split, attemptContext);

            // check
            while (recordReader.nextKeyValue()) {
                assert recordReader.getCurrentKey() != null;
                assert recordReader.getCurrentValue() != null;
                assert recordReader.getCurrentValue() instanceof BytesWritable;

                BytesWritable bytesWritable = (BytesWritable) recordReader.getCurrentValue();
                String kafkaEvent = new String(
                        java.util.Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength()), StandardCharsets.UTF_8);

                readEvents.add(kafkaEvent);
            }
        }

        assertEquals(NUMBER_EVENTS, readEvents.size());

        // Messages returns in order within a partition. So we sort all read messages
        // and check there are no dups or unexpected events
        Collections.sort(readEvents);
        for (int i=0; i<NUMBER_EVENTS; i++) {
            assertEquals("Incorrect message. Maybe there are duplicates or unexpected events?",
                    getMessageBody(i+1), readEvents.get(i));
        }
    }

}