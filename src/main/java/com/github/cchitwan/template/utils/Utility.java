package com.github.cchitwan.template.utils;

import java.io.Serializable;

/**
 * @author chanchal.chitwan on 07/04/17.
 */
public class Utility implements Serializable{

    public static void logMessageWithThreadId(Object log) {
        System.out.println(Thread.currentThread().getId() + " : " + log);
    }

}
