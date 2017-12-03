package com.github.cchitwan.template.processor;

import java.io.Serializable;

/**
 * @author chanchal.chitwan on 06/04/17.
 */
public interface IProcessor extends Serializable{
     void startJob() throws Exception;
}
