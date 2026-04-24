package com.github.cchitwan.template.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cchitwan.template.config.IConfig;
import com.github.cchitwan.template.utils.Constants;
import com.github.cchitwan.template.utils.Utility;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Structured Streaming replacement for the old DStream-based processor.
 * Implementors should update processAllBatches and can keep processEachPartion if they prefer partition-level handling.
 */
@Slf4j
@NoArgsConstructor
public abstract class BasicKafkaSingleStreamProcessor<T> implements IProcessor, Serializable {

    private IConfig config;
    private transient SparkSession spark;
    private final ObjectMapper objectMapper = Constants.objectMapper;
    private transient StreamingQuery query;
    private final AtomicBoolean started = new AtomicBoolean(false);

    protected BasicKafkaSingleStreamProcessor(IConfig config) {
        this.config = config;
    }

    protected <R> Dataset<R> getStructuredDataset(Class<R> clazz) {
        SparkConf sparkConf = new SparkConf().setAppName(config.getAppName());

        if (config.getMaster() != null && !config.getMaster().isEmpty()) {
            sparkConf.setMaster(config.getMaster());
        }

        spark = SparkSession.builder().config(sparkConf).getOrCreate();

        String bootstrap = config.getKafkaStreamConfig().getBrokerList();
        String topics = config.getKafkaStreamConfig().getTopic();

        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", topics)
                .option("startingOffsets", config.getStartingOffsets() != null ? config.getStartingOffsets() : "latest")
                .load()
                .selectExpr("CAST(value AS STRING) as value");

        Encoder<R> encoder = Encoders.bean(clazz);

        Dataset<R> parsed = raw
                .select("value")
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, R>) json -> {
                    try {
                        if (json == null || json.trim().isEmpty()) {
                            return java.util.Collections.<R>emptyList().iterator();
                        }
                        R obj = objectMapper.readValue(json, clazz);
                        return java.util.Collections.singletonList(obj).iterator();
                    } catch (Exception ex) {
                        Utility.logMessageWithThreadId("Failed to parse JSON payload: " + json + " error: " + ex.getMessage());
                        log.debug("Parsing error", ex);
                        return java.util.Collections.<R>emptyList().iterator();
                    }
                }, encoder);

        return parsed;
    }

    public void doJob(Class<T> clazz, HandlerLevel handlerLevel) throws Exception {
        if (started.getAndSet(true)) {
            log.warn("Processor already started.");
            return;
        }

        Dataset<T> ds = getStructuredDataset(clazz);

        String checkpointLocation = config.getCheckpointLocation();
        if (checkpointLocation == null || checkpointLocation.isEmpty()) {
            log.warn("No checkpoint location provided in config; checkpoints are required for reliable Structured Streaming offsets.");
            checkpointLocation = "/tmp/spark-checkpoint/" + config.getAppName();
        }

        query = ds.writeStream()
                .foreachBatch((VoidFunction2<Dataset<T>, Long>) (batchDataset, batchId) -> {
                    long start = System.currentTimeMillis();
                    Utility.logMessageWithThreadId("Batch " + batchId + " start at " + start + " size=" + batchDataset.count());

                    try {
                        if (handlerLevel == null || HandlerLevel.RDD.equals(handlerLevel)) {
                            processAllBatches(batchDataset);
                        } else {
                            batchDataset.foreachPartition((ForeachPartitionFunction<T>) partitionIterator -> {
                                try {
                                    processEachPartion(partitionIterator);
                                } catch (Exception ex) {
                                    Utility.logMessageWithThreadId("Exception in partition processing: " + ex.getMessage());
                                    log.error("partition processing error", ex);
                                }
                            });
                        }
                    } catch (Exception ex) {
                        Utility.logMessageWithThreadId("Exception in foreachBatch processing: " + ex.getMessage());
                        log.error("foreachBatch processing error", ex);
                        throw ex;
                    } finally {
                        long stop = System.currentTimeMillis();
                        Utility.logMessageWithThreadId("Batch " + batchId + " finished in " + (stop - start) + "ms");
                    }
                })
                .option("checkpointLocation", checkpointLocation)
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            log.error("Streaming query terminated with exception", e);
            throw e;
        }
    }

    protected abstract void processEachPartion(final Iterator<T> records);

    protected abstract void processAllBatches(Dataset<T> batchDataset);

    public enum HandlerLevel {
        RDD, PARTION
    }
}
