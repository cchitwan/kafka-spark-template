package com.github.cchitwan.template.config;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author chanchal.chitwan on 06/04/17.
 *
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IConfig implements Serializable {
    private String master;

    private String appName;

    private int slidingIntervalSec;

    private int executers;

    private HttpConnectionManagerConfig httpConnectionManagerConfig;

    private KafkaUpstreamConfig kafkaStreamConfig;
    private long myOffsetRange = 1000;
    private boolean enableKafkaOffset = false;

    private boolean enableConfigLogging = false;

    /**
     * Structured Streaming: checkpoint location used for committing offsets and recovery. Required for production.
     */
    private String checkpointLocation;

    /**
     * structured streaming starting offsets, e.g. "earliest" or "latest" or a json string of offsets. Defaults to "latest".
     */
    private String startingOffsets = "latest";

    public String getParentConfig(){
        return enableConfigLogging==true?this.toString():"{config logging is disabled}";
    }
}
