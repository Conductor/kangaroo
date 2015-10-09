package com.conductor.kafka.hadoop;

import com.conductor.kafka.IntTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by Greg Temchenko on 10/5/15.
 */
public class KafkaInputFormatIntTest extends IntTestBase {

    @Test
    public void getAllSplits() throws Exception {
        KafkaInputFormat kafkaInputFormat = new KafkaInputFormat();

        Configuration conf = new Configuration();
        conf.set("kafka.zk.connect", "localhost:" + Integer.valueOf(zkPort));
        conf.set("kafka.topic", TEST_TOPIC);
        conf.set("kafka.groupid", TEST_GROUP);

        JobConf jobConf = new JobConf(conf);
        JobContext jobContext = new JobContextImpl(jobConf, JobID.forName("job_test_123"));
        List<InputSplit> splits = kafkaInputFormat.getSplits(jobContext);

        // check
        assert splits.size() > 0;

        int sum = 0;
        for (InputSplit split : splits) {
            sum += split.getLength();
        }
        assertEquals(100, sum);
    }

}