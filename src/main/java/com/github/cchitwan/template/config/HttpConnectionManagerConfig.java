package com.github.cchitwan.template.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class HttpConnectionManagerConfig implements Serializable {

    private Long connectionTimeout;
    private Integer defaultMaxConnectionsPerHost;
    private Integer maxTotalConnections;
    private int soTimeout;
    private boolean soReuseAddress;
    private Integer soLinger;
    private boolean soKeepAlive;
    private boolean tcpNoDelay;
}
