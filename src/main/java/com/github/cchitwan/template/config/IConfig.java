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
    public String getParentConfig(){
        return enableConfigLogging==true?this.toString():"{config logging is disabled}";
    }
}
