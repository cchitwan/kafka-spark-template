package com.github.cchitwan.template.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class KafkaPayload implements Serializable{

    private String value;
}
