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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.conductor.hadoop.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A {@link InputFormat} that can read any number of Kafka queues in a single {@link Job}.
 * 
 * <p>
 * Note, you <em>MUST</em> use {@link DelegatingMapper} when setting {@link Job#setMapperClass(Class)} in order for this
 * {@link InputFormat} work. This is set for you every time you call {@link #addTopic(Job, String, String, Class)}, but
 * be sure not to override it!
 * 
 * <p>
 * This class may be used safely in combination with {@link org.apache.hadoop.mapreduce.lib.input.MultipleInputs}.
 * 
 * @see KafkaInputFormat
 * @see KafkaInputSplit
 * @see KafkaRecordReader
 * 
 * @author <a href="mailto:cgreen@conductor.com">Casey Green</a>
 */
public class MultipleKafkaInputFormat extends InputFormat<LongWritable, BytesWritable> {

    static final Logger LOG = LoggerFactory.getLogger(KafkaInputFormat.class);

    private static final String TOPICS_CONF = "kafka.topics";

    /**
     * Creates input splits for each {@link TopicConf} set up by {@link #addTopic(Job, String, String, Class)}.
     * 
     * <p>
     * {@inheritDoc}
     */
    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final List<InputSplit> splits = Lists.newArrayList();
        final List<TopicConf> topicConfs = getTopics(conf);
        warnOnDuplicateTopicConsumers(topicConfs);
        for (final TopicConf topicConf : topicConfs) {
            final String topic = topicConf.getTopic();
            final String group = topicConf.getConsumerGroup();
            final Class<? extends Mapper> delegateMapper = topicConf.getMapper();
            for (final InputSplit inputSplit : getInputSplits(conf, group, topic)) {
                splits.add(new TaggedInputSplit(inputSplit, conf, KafkaInputFormat.class, delegateMapper));
            }
        }
        return splits;
    }

    @VisibleForTesting
    List<InputSplit> getInputSplits(final Configuration conf, final String group, final String topic)
            throws IOException {
        return new KafkaInputFormat().getInputSplits(conf, topic, group);
    }

    private void warnOnDuplicateTopicConsumers(final List<TopicConf> topicConfs) {
        final Set<String> topicConsumers = Sets.newHashSet();
        for (final TopicConf topicConf : topicConfs) {
            final String topicConsumer = topicConf.getTopic() + topicConf.getConsumerGroup();
            if (topicConsumers.contains(topicConsumer)) {
                LOG.warn(format(
                        "Found duplicate consumer group '%s' consuming the same topic '%s'! "
                                + "This may cause non-deterministic behavior if you commit your offsets for this consumer group!",
                        topicConf.getConsumerGroup(), topicConf.getTopic()));
            }
            topicConsumers.add(topicConsumer);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(final InputSplit split,
            final TaskAttemptContext context) throws IOException, InterruptedException {
        final TaggedInputSplit taggedInputSplit = (TaggedInputSplit) split;
        final TaskAttemptContext taskAttemptContextClone = new TaskAttemptContextImpl(taggedInputSplit.getConf(),
                context.getTaskAttemptID());
        taskAttemptContextClone.setStatus(context.getStatus());
        return new DelegatingRecordReader<LongWritable, BytesWritable>(split, taskAttemptContextClone);
    }

    /**
     * Returns a {@link List} containing <em>all</em> of the topic-group-{@link Mapper} combinations added via
     * {@link #addTopic(Job, String, String, Class)}.
     * 
     * @param conf
     *            the conf for this job.
     * @return all of the configured {@link TopicConf}s
     */
    @SuppressWarnings("unchecked")
    public static List<TopicConf> getTopics(final Configuration conf) {
        final List<TopicConf> result = Lists.newArrayList();
        for (final String topicConf : conf.get(TOPICS_CONF).split(";")) {
            final String[] topicConfTokens = topicConf.split(",");
            final String topic = topicConfTokens[0];
            final String group = topicConfTokens[1];
            final Class<? extends Mapper> mapper;
            try {
                mapper = (Class<? extends Mapper>) conf.getClassByName(topicConfTokens[2]);
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            result.add(new TopicConf(topic, group, mapper));
        }
        return result;
    }

    /**
     * Adds a topic input that will be read with the provided {@code mapperClass}. This method also sets the job-level
     * mapper ({@link Job#setMapperClass(Class)}) to {@link DelegatingMapper}, which is required for this input format
     * to work, so <em>do not override this</em>!
     * <p>
     * Note that you can read the same topic using any number of {@link Mapper}s, same of different. Although odd, it is
     * possible to read the same queue with the same {@link Mapper}.
     * 
     * @param job
     *            the job
     * @param topic
     *            the topic to read
     * @param consumerGroup
     *            the consumer group for this particular input configuration.
     * @param mapperClass
     *            the mapper class that will read the topic
     */
    public static void addTopic(final Job job, final String topic, final String consumerGroup,
            final Class<? extends Mapper> mapperClass) {
        job.setMapperClass(DelegatingMapper.class);
        final String existingTopicConf = job.getConfiguration().get(TOPICS_CONF);
        final String topicConfig = format("%s,%s,%s", topic, consumerGroup, mapperClass.getName());
        if (Strings.isNullOrEmpty(existingTopicConf)) {
            job.getConfiguration().set(TOPICS_CONF, topicConfig);
        } else {
            job.getConfiguration().set(TOPICS_CONF, format("%s;%s", existingTopicConf, topicConfig));
        }
    }

    /**
     * Represents a set of Kafka input to a Map/Reduce job, namely a topic and a {@link Mapper}.
     */
    public static class TopicConf {
        private final String topic;
        private final String consumerGroup;
        private final Class<? extends Mapper> mapper;

        public TopicConf(final String topic, final String consumerGroup, final Class<? extends Mapper> mapper) {
            this.topic = topic;
            this.consumerGroup = consumerGroup;
            this.mapper = mapper;
        }

        public String getTopic() {
            return topic;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public Class<? extends Mapper> getMapper() {
            return mapper;
        }

        @Override
        public String toString() {
            return format("group %s consuming %s mapped by %s", consumerGroup, topic, mapper);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof TopicConf))
                return false;

            final TopicConf topicConf = (TopicConf) o;

            if (consumerGroup != null ? !consumerGroup.equals(topicConf.consumerGroup)
                    : topicConf.consumerGroup != null)
                return false;
            if (mapper != null ? !mapper.equals(topicConf.mapper) : topicConf.mapper != null)
                return false;
            if (topic != null ? !topic.equals(topicConf.topic) : topicConf.topic != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = topic != null ? topic.hashCode() : 0;
            result = 31 * result + (consumerGroup != null ? consumerGroup.hashCode() : 0);
            result = 31 * result + (mapper != null ? mapper.hashCode() : 0);
            return result;
        }
    }
}
