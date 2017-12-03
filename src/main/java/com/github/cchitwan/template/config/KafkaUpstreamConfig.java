package com.github.cchitwan.template.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

/**
 * @author chanchal.chitwan on 06/04/17.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KafkaUpstreamConfig
        implements IKafkaOffsetManagementConfig {

    @NotNull
    private String brokerList;

    @NotNull
    private String zookeeperConnectionString;

    @NotNull
    private Integer zookeeperConnectTimeout;

    @NotNull
    private String consumerGroup;

    @NotNull
    private String topic;
}
