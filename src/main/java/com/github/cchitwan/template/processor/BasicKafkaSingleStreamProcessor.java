package com.github.cchitwan.template.processor;

import com.github.cchitwan.template.utils.Constants;
import com.google.common.base.Strings;
import com.github.cchitwan.template.config.IConfig;
import com.github.cchitwan.template.kafka.KafkaOffsetsDao;
import com.github.cchitwan.template.kafka.KafkaPayload;
import com.github.cchitwan.template.utils.Utility;
import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chanchal.chitwan on 06/04/17.
 */
@Data
@NoArgsConstructor
public abstract class BasicKafkaSingleStreamProcessor<T> implements IProcessor {
    private IConfig config;
    private transient JavaStreamingContext javaStreamingContext;
    private transient KafkaOffsetsDao kafkaOffsetsDao;
    protected BasicKafkaSingleStreamProcessor(IConfig config){
        this.config=config;
        if(config.isEnableKafkaOffset()) {
            kafkaOffsetsDao = new KafkaOffsetsDao(config.getKafkaStreamConfig(), config.getAppName(), config.getMyOffsetRange());
        }
    }



    protected JavaDStream<T> getCustomJavaStream(Class<T> t) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName(config.getAppName())
                                             .setMaster(config.getMaster())
                                             .set("spark.streaming.concurrentJobs", String.valueOf(config.getExecuters()))
                                             .set("spark.driver.allowMultipleContexts", "true");

        javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(config.getSlidingIntervalSec()));
        try {

            Set<String> topicSet = new HashSet<>(Arrays.asList(config.getKafkaStreamConfig()
                                                                     .getTopic()
                                                                     .split(Constants.DELIMITER)));
            Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("metadata.broker.list", config.getKafkaStreamConfig()
                                                          .getBrokerList());
            kafkaParams.put("zookeeper.connect", config.getKafkaStreamConfig()
                                                       .getZookeeperConnectionString());
            kafkaParams.put("group.id", config.getKafkaStreamConfig().getConsumerGroup());
            Map<TopicAndPartition, Long> fromOffsets = new HashMap<>();
            if(config.isEnableKafkaOffset()) {
                System.out.println("Storing kafka offset");
                OffsetRange[] offsetRangesToRead = kafkaOffsetsDao.getOffsetRangeForRead(config.getKafkaStreamConfig()
                                                                                               .getTopic());
                for (OffsetRange offsetRangeToRead : offsetRangesToRead) {
                    fromOffsets.put(offsetRangeToRead.topicAndPartition(), offsetRangeToRead.fromOffset());
                }

                JavaDStream<T> data = KafkaUtils.createDirectStream(
                                javaStreamingContext,
                                String.class,
                                String.class,
                                StringDecoder.class,
                                StringDecoder.class,
                                t,
                                kafkaParams,
                                fromOffsets,
                                messageAndMetadata -> Constants.objectMapper.readValue(
                                        (new KafkaPayload(messageAndMetadata.message())).getValue(),
                                        t)
                        );
//                System.out.println("Data with offset: "+data);
                return data;

            }else {
                System.out.println("Not storing kafka offset");
                JavaDStream<KafkaPayload>data = KafkaUtils.createDirectStream(javaStreamingContext,
                                                     String.class,
                                                     String.class,
                                                     StringDecoder.class,
                                                     StringDecoder.class,
                                                     kafkaParams,
                                                     topicSet)
                                 .map((org.apache.spark.api.java.function.Function<Tuple2<String, String>, KafkaPayload>) dstream -> new KafkaPayload(dstream._2()));
                JavaDStream<T> tJavaDStream = data.map(v1 -> {
                    try {
                        return !Strings.isNullOrEmpty(v1.getValue()) ? Constants.objectMapper.readValue(v1.getValue(), t) : null;
                    } catch (Exception e) {
                        Utility.logMessageWithThreadId("Errored kafka Payload " + v1);
                        Utility.logMessageWithThreadId("Error converting kafka payload to varys event " + e);
                        Arrays.stream(e.getStackTrace()).forEach(System.out::println);
                        return null;
                    }
                });

                return tJavaDStream;
            }



        }catch (Exception e){
            System.out.println("Exception caught in getCustomJavaStream: "+e.getStackTrace().toString());
            throw e;
        }
    }

    protected void processEachRDD(JavaDStream<T> javaDStream){
        final AtomicReference<OffsetRange[]> offsetRangesReference = new AtomicReference<>();
        try {

            javaDStream.foreachRDD(rdd -> {
                Utility.logMessageWithThreadId(" Batch time to start " + System.currentTimeMillis());
                if(config.isEnableKafkaOffset()) {
                    offsetRangesReference.set(((HasOffsetRanges) rdd.rdd()).offsetRanges());
                }
                rdd.foreachPartition(records -> {
                    try {
                        processEachPartion(records);
                    }catch (Exception e){
                        Utility.logMessageWithThreadId("Exception caught at partion level");
                        Arrays.stream(e.getStackTrace()).forEach(System.out::println);
                    }
                });
                Utility.logMessageWithThreadId("Batch time to stop " + System.currentTimeMillis());
                if(config.isEnableKafkaOffset()) {
                    for (OffsetRange offsetRange : offsetRangesReference.get()) {
                        kafkaOffsetsDao.storeOffsets(offsetRange, offsetRange.topic());
                        System.out.println("store offset range " + offsetRange);
                    }
                }

            });
        }catch (Exception e){
            Utility.logMessageWithThreadId("Exception caught at RDD level");
            Arrays.stream(e.getStackTrace()).forEach(System.out::println);
        }
    }

    /**
     * Kafka offset save is taken care already by default.
     * @param records
     */
    protected abstract void processEachPartion(final Iterator<T> records);

    /**
     * Kafka offset save is not take care by default.
     * @param javaDStream
     */
    protected abstract void processAllRDDs(JavaDStream<T> javaDStream);

    public void doJob(Class<T> t, HandlerLevel handlerLevel) throws Exception {
        Utility.logMessageWithThreadId("Started BasicKafkaStreamProcessor...");
        JavaDStream<T> tJavaDStream = getCustomJavaStream(t);
        if(handlerLevel ==null || HandlerLevel.RDD.equals(handlerLevel)) {
            Utility.logMessageWithThreadId("Started processAllRDDs...");
            processAllRDDs(tJavaDStream);
        }else {
            Utility.logMessageWithThreadId("Started processEachRDD...");
            processEachRDD(tJavaDStream);
        }
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

    public static enum HandlerLevel {
        RDD, PARTION
    }
    @Deprecated
    public static KafkaPayload mapKafkaPayload(String data) {

        KafkaPayload kafkaPayload = new KafkaPayload(data);
        if (Strings.isNullOrEmpty(kafkaPayload.getValue())) {
            return null;
        }

        KafkaPayload event = null;
        try {
            event = Constants.objectMapper.readValue((kafkaPayload).getValue(), KafkaPayload.class);
        } catch (IOException ex) {
            System.out.println("wrong payload " + data);
            ex.printStackTrace();
        }

        return event;
    }


}

