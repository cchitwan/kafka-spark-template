package com.github.cchitwan.template.config;

import java.io.Serializable;

/**
 * @author chanchal.chitwan on 06/04/17.
 */
public interface IKafkaOffsetManagementConfig
        extends Serializable {

    String getZookeeperConnectionString();

    String getConsumerGroup();

    Integer getZookeeperConnectTimeout();
}
