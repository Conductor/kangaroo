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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import com.conductor.kafka.hadoop.MultipleKafkaInputFormat.TopicConf;
import com.google.common.annotations.Beta;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * {@link KafkaJobBuilder} is an attempt to make Map/Reduce jobs over Kafka queues easier to configure.
 * 
 * <p>
 * Usage of this class:
 * <ol>
 * <li>
 * Create a new builder using {@link #newBuilder()}.</li>
 * <li>
 * Set the job configurations on the builder instance.</li>
 * <li>
 * Create any number of {@link Job} instances by calling {@link #configureJob(Configuration)}.</li>
 * <li>
 * Any additional job setup can be set on the resulting {@link Job} instance.</li>
 * </ol>
 * 
 * <p>
 * The following are required settings:
 * <ol>
 * <li>The Zookeeper connection string: {@link #setZkConnect(String)}</li>
 * <li>At least one queue input: {@link #addQueueInput(String, String, Class)}</li>
 * <li>One output format: {@link #setNullOutputFormat()}, {@link #setTextFileOutputFormat()}, or
 * {@link #setSequenceFileOutputFormat()}. If the output path is not specified, one will be generated for you.</li>
 * <li>If your output path is S3, you must also specify your S3 credentials using {@link #useS3(String, String, String)}
 * , where {@code defaultS3Bucket} is optional if and only if you have specified the full path of your output.
 * Otherwise, {@code defaultS3Bucket} will be used to <em>generate</em> an output path.</li>
 * </ol>
 * 
 * <p>
 * Note that calling {@link #configureJob(Configuration)} has no side effects on the instance of the
 * {@link KafkaJobBuilder}, so it is more like a "builder factory" in that sense; you can call
 * {@link #configureJob(Configuration)} as many times as you want, changing job parameters in between calls if you so
 * choose.
 * 
 * @see KafkaInputFormat
 * @see MultipleKafkaInputFormat
 * 
 * @author <a href="mailto:cgreen@conductor.com">Casey Green</a>
 */
@Beta
public final class KafkaJobBuilder {

    private static enum SupportedOutputFormat {
        NULL, TEXT_FILE, SEQUENCE_FILE
    }

    private String jobName;
    private List<TopicConf> queueMappers = Lists.newArrayList();
    private Class<?> mapOutputKeyClass;
    private Class<?> mapOutputValueClass;
    private Class<? extends Partitioner> partitionerClass;
    private Class<? extends Reducer> reducerClass;
    private Class<? extends OutputFormat> outputFormatClass;
    private Class<?> outputKeyClass;
    private Class<?> outputValueClass;
    private boolean lazyOutputFormat;
    private SupportedOutputFormat outputFormat;
    private String zkConnect;
    private String taskMemorySettings;
    private int numReduceTasks = 10;
    private int kafkaFetchSizeBytes = 5 * 1024 * 1024;
    private boolean useS3 = false;
    private String s3Bucket;
    private String s3AccessKey;
    private String s3SecretyKey;
    private String outputFormatPath;

    // enforce use of the builder
    private KafkaJobBuilder() {
    }

    /**
     * Creates a {@link Job} based on how {@code this} {@link KafkaJobBuilder} has been configured. There are no
     * side-effects on {@code this} instance when you call this method, so you can call it multiple times.
     * 
     * @param conf
     *            the job conf.
     * @return a fully configured {@link Job}.
     * @throws Exception error
     * @throws IllegalArgumentException
     *             if any required parameters are not set.
     */
    public Job configureJob(final Configuration conf) throws Exception {
        validateSettings();
        final Job job = Job.getInstance(conf, getDefaultedJobName());

        // set queue inputs
        if (getQueueMappers().size() == 1) {
            job.setInputFormatClass(KafkaInputFormat.class);
            final TopicConf topicConf = Iterables.getOnlyElement(getQueueMappers());
            KafkaInputFormat.setTopic(job, topicConf.getTopic());
            KafkaInputFormat.setConsumerGroup(job, topicConf.getConsumerGroup());
            job.setMapperClass(topicConf.getMapper());
        } else {
            job.setInputFormatClass(MultipleKafkaInputFormat.class);
            for (final TopicConf topicConf : getQueueMappers()) {
                MultipleKafkaInputFormat.addTopic(job, topicConf.getTopic(), topicConf.getConsumerGroup(),
                        topicConf.getMapper());
            }
        }

        if (getMapOutputKeyClass() != null) {
            job.setMapOutputKeyClass(getMapOutputKeyClass());
        }

        if (getMapOutputValueClass() != null) {
            job.setMapOutputValueClass(getMapOutputValueClass());
        }

        if (getReducerClass() == null) {
            job.setNumReduceTasks(0);
        } else {
            job.setReducerClass(getReducerClass());
            job.setNumReduceTasks(getNumReduceTasks());
        }

        if (getPartitionerClass() != null) {
            job.setPartitionerClass(getPartitionerClass());
        }

        // set output
        job.setOutputFormatClass(getOutputFormatClass());
        job.setOutputKeyClass(getOutputKeyClass());
        job.setOutputValueClass(getOutputValueClass());
        if (getOutputFormat() == SupportedOutputFormat.TEXT_FILE) {
            TextOutputFormat.setOutputPath(job, getDefaultedOutputPath());
        } else if (getOutputFormat() == SupportedOutputFormat.SEQUENCE_FILE) {
            SequenceFileOutputFormat.setOutputPath(job, getDefaultedOutputPath());
        }

        if (usingS3()) {
            job.getConfiguration().set("fs.s3n.awsAccessKeyId", getS3AccessKey());
            job.getConfiguration().set("fs.s3n.awsSecretAccessKey", getS3SecretyKey());
            job.getConfiguration().set("fs.s3.awsAccessKeyId", getS3AccessKey());
            job.getConfiguration().set("fs.s3.awsSecretAccessKey", getS3SecretyKey());
        }

        if (isLazyOutputFormat()) {
            LazyOutputFormat.setOutputFormatClass(job, getOutputFormatClass());
        }

        // setup kafka input format specifics
        KafkaInputFormat.setZkConnect(job, getZkConnect());
        KafkaInputFormat.setKafkaFetchSizeBytes(job, getKafkaFetchSizeBytes());

        job.setSpeculativeExecution(false);
        job.setJarByClass(getClass());

        // memory settings for mappers
        if (!Strings.isNullOrEmpty(getTaskMemorySettings())) {
            job.getConfiguration().set("mapred.child.java.opts", getTaskMemorySettings());
        }

        return job;
    }

    private String getDefaultedJobName() {
        final String jobName;
        if (Strings.isNullOrEmpty(getJobName())) {
            jobName = generateJobName();
        } else {
            jobName = getJobName();
        }
        return jobName;
    }

    private Path getDefaultedOutputPath() throws Exception {
        if (!Strings.isNullOrEmpty(getOutputFormatPath())) {
            return new Path(getOutputFormatPath());
        } else {
            if (usingS3()) {
                return new Path(String.format("s3://%s/%s", getS3Bucket(), generateOutputDirectory()));
            } else {
                return new Path(generateOutputDirectory());
            }
        }
    }

    private String generateOutputDirectory() {
        return String.format("%s_%tF_%<tH%<tM%<tS", getJobName(), new Date());
    }

    /**
     * Creates a new builder.
     * 
     * @return {@code this}
     */
    public static KafkaJobBuilder newBuilder() {
        return new KafkaJobBuilder();
    }

    /**
     * Sets the name of the job (optional; a name will be generated if one is not supplied).
     * 
     * @param jobName
     *            job name.
     * @return {@code this}
     */
    public KafkaJobBuilder setJobName(final String jobName) {
        this.jobName = jobName;
        return this;
    }

    /**
     * Adds a queue input to the job (required).
     * 
     * @param queueName
     *            the queue to consume.
     * @param consumerGroup
     *            the consumer group reading this queue.
     * @param mapper
     *            the mapper used to read the queue.
     * @return {@code this}
     */
    public KafkaJobBuilder addQueueInput(final String queueName, final String consumerGroup,
            final Class<? extends Mapper> mapper) {
        checkArgument(!Strings.isNullOrEmpty(queueName), "queueName is blank or null.");
        checkArgument(!Strings.isNullOrEmpty(consumerGroup), "consumerGroup is blank or null.");
        getQueueMappers().add(new TopicConf(queueName, consumerGroup, mapper));
        return this;
    }

    /**
     * Sets the map output key of the job (optional).
     * 
     * @param mapOutputKeyClass
     *            the map output key class.
     * @return {@code this}
     */
    public KafkaJobBuilder setMapOutputKeyClass(final Class<?> mapOutputKeyClass) {
        this.mapOutputKeyClass = mapOutputKeyClass;
        return this;
    }

    /**
     * Sets the map output value of the job (optional).
     * 
     * @param mapOutputValueClass
     *            map output value class of the job.
     * @return {@code this}
     */
    public KafkaJobBuilder setMapOutputValueClass(final Class<?> mapOutputValueClass) {
        this.mapOutputValueClass = mapOutputValueClass;
        return this;
    }

    /**
     * Sets the {@link Reducer} for this job (optional).
     * 
     * @param reducerClass
     *            the {@link Reducer} class.
     * @return {@code this}
     */
    public KafkaJobBuilder setReducerClass(final Class<? extends Reducer> reducerClass) {
        this.reducerClass = reducerClass;
        return this;
    }

    /**
     * Sets the Zookeeper connection string for Kafka (required).
     * 
     * @param zkConnect
     *            the connection string.
     * @return {@code this}
     * @see KafkaInputFormat#setZkConnect(Job, String)
     */
    public KafkaJobBuilder setZkConnect(final String zkConnect) {
        this.zkConnect = zkConnect;
        return this;
    }

    /**
     * Sets the {@code mapred.child.java.opts} of the job (optional).
     * 
     * @param taskMemorySettings
     *            memory settings, e.g. {@code -Xmx2048m -XX:MaxPermSize=256M}.
     * @return {@code this}
     */
    public KafkaJobBuilder setTaskMemorySettings(final String taskMemorySettings) {
        this.taskMemorySettings = taskMemorySettings;
        return this;
    }

    /**
     * Sets the number of reduce tasks to use (optional).
     * 
     * <p>
     * You do not need to set this to {@code 0} if you are not using a {@link Reducer} - the builder will infer that.
     * 
     * @param numReduceTasks
     *            number of reduce tasks to use.
     * @return {@code this}
     */
    public KafkaJobBuilder setNumReduceTasks(final int numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
        return this;
    }

    /**
     * Sets the {@link Partitioner} of this job (optional).
     * 
     * @param partitioner
     *            the partition class.
     * @return {@code this}
     */
    public KafkaJobBuilder setParitioner(final Class<? extends Partitioner> partitioner) {
        this.partitionerClass = partitioner;
        return this;
    }

    /**
     * Sets the Kafka fetch size in bytes (optional, defaults to {@link KafkaInputFormat#DEFAULT_FETCH_SIZE_BYTES}).
     * 
     * @param kafkaFetchSizeBytes
     *            fetch size in bytes.
     * @return {@code this}
     * @see KafkaInputFormat#setKafkaFetchSizeBytes(Job, int)
     */
    public KafkaJobBuilder setKafkaFetchSizeBytes(final int kafkaFetchSizeBytes) {
        this.kafkaFetchSizeBytes = kafkaFetchSizeBytes;
        return this;
    }

    /**
     * Job will use {@link NullOutputFormat}.
     * 
     * <p>
     * Note that {@link NullWritable} is used for output key and value.
     * 
     * @return {@code this}
     */
    public KafkaJobBuilder setNullOutputFormat() {
        return configureOutput(null, NullOutputFormat.class, NullWritable.class, NullWritable.class,
                SupportedOutputFormat.NULL);
    }

    /**
     * Job will use {@link TextOutputFormat}, using the fully specified {@code outputPath} if it is not null. Otherwise
     * the job will generate an output path as specified by {@link #setTextFileOutputFormat()}.
     * <p>
     * Note that {@link Text} is used for output key and value.
     * 
     * @param outputPath
     *            (optional) the fully specified output path of the job.
     * @return {@code this}
     */
    public KafkaJobBuilder setTextFileOutputFormat(@Nullable final String outputPath) {
        return configureOutput(outputPath, TextOutputFormat.class, Text.class, Text.class,
                SupportedOutputFormat.TEXT_FILE);
    }

    /**
     * Job will use {@link TextOutputFormat}, generating a unique output path either under the user's HDFS home
     * directory, or under {@link #getS3Bucket()} if {@link #useS3(String, String, String)} was set.
     * 
     * <p>
     * Note that {@link Text} is used for output key and value.
     * 
     * @return {@code this}
     */
    public KafkaJobBuilder setTextFileOutputFormat() {
        return setTextFileOutputFormat(null);
    }

    /**
     * Job will use {@link SequenceFileOutputFormat}, using the fully specified {@code outputPath} if it is not null.
     * Otherwise the job will generate an output path as specified by {@link #setSequenceFileOutputFormat()}.
     * 
     * <p>
     * Note that {@link BytesWritable} is used for output key and value.
     * 
     * @param outputPath
     *            (optional) the fully specified output path of the job.
     * @return {@code this}
     */
    public KafkaJobBuilder setSequenceFileOutputFormat(@Nullable final String outputPath) {
        return configureOutput(outputPath, SequenceFileOutputFormat.class, BytesWritable.class, BytesWritable.class,
                SupportedOutputFormat.SEQUENCE_FILE);
    }

    /**
     * Job will use {@link SequenceFileOutputFormat}, generating a unique output path either under the user's HDFS home
     * directory, or under {@link #getS3Bucket()} if {@link #useS3(String, String, String)} was set.
     * <p>
     * Note that {@link BytesWritable} is used for output key and value.
     * 
     * @return {@code this}
     */
    public KafkaJobBuilder setSequenceFileOutputFormat() {
        return setSequenceFileOutputFormat(null);
    }

    private KafkaJobBuilder configureOutput(final String outputPath,
            final Class<? extends OutputFormat> outputFormatClass, final Class<?> outputKeyClass,
            final Class<?> outputValueClass, final SupportedOutputFormat outputFormat) {
        this.outputFormatClass = outputFormatClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
        this.outputFormatPath = outputPath;
        this.outputFormat = outputFormat;
        return this;
    }

    /**
     * Job will set {@link LazyOutputFormat#setOutputFormatClass(Job, Class)}.
     * 
     * @return {@code this}
     */
    public KafkaJobBuilder setUseLazyOutput() {
        this.lazyOutputFormat = true;
        return this;
    }

    /**
     * Indicates that you intend to use S3 as a target for your output.
     * <p>
     * If you specify {@link #setSequenceFileOutputFormat()} in addition to this method, an output path will be
     * generated and put into the {@code defaultBucket}.
     * 
     * @param accessKeyId
     *            S3 access key, which requires read/write/delete permissions on the output path.
     * @param secretKey
     *            S3 secret key
     * @param defaultBucket
     *            the default bucket to use if no path is explicitly specified.
     * @return {@code this}
     */
    public KafkaJobBuilder useS3(final String accessKeyId, final String secretKey, @Nullable final String defaultBucket) {
        checkNotNull(accessKeyId, "accessKeyId is null.");
        checkNotNull(accessKeyId, "secretKey is null.");
        this.useS3 = true;
        this.s3AccessKey = accessKeyId;
        this.s3SecretyKey = secretKey;
        this.s3Bucket = defaultBucket;
        return this;
    }

    private void validateSettings() {
        checkArgument(!Strings.isNullOrEmpty(getZkConnect()), "Did not specify a Zookeeper connection string");
        checkArgument(!getQueueMappers().isEmpty(), "Did not specify input queue+mapper.");
        checkArgument(getOutputFormat() != null, "Did not specify an output format.");
        // if no output dir specified, must at least specify a bucket.
        if (usingS3() && Strings.isNullOrEmpty(getOutputFormatPath())) {
            checkArgument(!Strings.isNullOrEmpty(getS3Bucket()), "Specified s3 output, but no bucket.");
        }
        if (getOutputFormatPath() != null
                && (getOutputFormatPath().startsWith("s3://") || getOutputFormatPath().startsWith("s3n://"))
                && !usingS3()) {
            checkArgument(false, "Specified s3 output, but no credentials.");
        }
    }

    private String generateJobName() {
        final StringBuilder jobName = new StringBuilder();
        for (final TopicConf map : getQueueMappers()) {
            if (jobName.length() > 0) {
                jobName.append(" + ");
            }
            jobName.append(String.format("queue %s mapped by %s", map.getTopic(), map.getMapper().getSimpleName()));
        }
        if (getReducerClass() != null) {
            jobName.append(String.format(" -> reduced by %s", getReducerClass().getSimpleName()));
        }
        return jobName.toString();
    }

    public String getJobName() {
        return jobName;
    }

    public List<TopicConf> getQueueMappers() {
        return queueMappers;
    }

    public Class<?> getMapOutputKeyClass() {
        return mapOutputKeyClass;
    }

    public Class<?> getMapOutputValueClass() {
        return mapOutputValueClass;
    }

    public Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }

    public String getZkConnect() {
        return zkConnect;
    }

    public String getTaskMemorySettings() {
        return taskMemorySettings;
    }

    public int getNumReduceTasks() {
        return numReduceTasks;
    }

    public int getKafkaFetchSizeBytes() {
        return kafkaFetchSizeBytes;
    }

    public Class<? extends OutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }

    public Class<?> getOutputKeyClass() {
        return outputKeyClass;
    }

    public Class<?> getOutputValueClass() {
        return outputValueClass;
    }

    private SupportedOutputFormat getOutputFormat() {
        return outputFormat;
    }

    public boolean usingS3() {
        return useS3;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getOutputFormatPath() {
        return outputFormatPath;
    }

    public Class<? extends Partitioner> getPartitionerClass() {
        return partitionerClass;
    }

    public boolean isLazyOutputFormat() {
        return lazyOutputFormat;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretyKey() {
        return s3SecretyKey;
    }
}
