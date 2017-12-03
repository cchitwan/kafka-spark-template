package com.github.cchitwan.template.kafka;

import lombok.Data;

import java.io.Serializable;

/**
 * @author chanchal.chitwan on 06/04/17.
 */
@Data
public class OffsetRange
        implements Serializable {

    public String topic;
    public int partition;
    public long fromOffsetRange;
    public long untilOffsetRange;

    public static OffsetRange fromOffsetRange(org.apache.spark.streaming.kafka.OffsetRange or) {
        OffsetRange newOr = new OffsetRange();
        newOr.setTopic(or.topic());
        newOr.setPartition(or.partition());
        newOr.setFromOffsetRange(or.fromOffset());
        newOr.setUntilOffsetRange(or.untilOffset());
        return newOr;
    }

    public static org.apache.spark.streaming.kafka.OffsetRange toOffsetRange(OffsetRange or) {
        org.apache.spark.streaming.kafka.OffsetRange newOr =
                org.apache.spark.streaming.kafka.OffsetRange.apply(or.topic, or.partition, or.fromOffsetRange, or.untilOffsetRange);
        return newOr;
    }
}
