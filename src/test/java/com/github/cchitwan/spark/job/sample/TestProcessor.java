package com.github.cchitwan.spark.job.sample;

import com.github.cchitwan.template.processor.BasicKafkaSingleStreamProcessor;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.Iterator;

/**
 * @author chanchal.chitwan on 07/04/17.
 */
public class TestProcessor
        extends BasicKafkaSingleStreamProcessor<TestEvent> {

    public TestProcessor(TestConfig config){
        super(config);
    }

    @Override
    protected void processAllRDDs(final JavaDStream javaDStream) {
        System.out.println("Processing RDD by TestProcessor. Can you tell me more how to process each RDD");
    }

    @Override
    public void startJob() throws Exception {
        super.doJob(TestEvent.class, HandlerLevel.PARTION);
    }

    @Override
    protected void processEachPartion(final Iterator<TestEvent> records) {
        System.out.println("Processing each partion by TestProcessor. Can you tell me more how to process each partion");
    }
}
