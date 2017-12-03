package com.github.cchitwan.spark.job.sample;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serializable;

/**
 * @author chanchal.chitwan on 09/04/17.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TestEvent implements Serializable{
    private String testField;
}
