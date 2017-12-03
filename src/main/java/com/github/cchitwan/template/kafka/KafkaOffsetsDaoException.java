package com.github.cchitwan.template.kafka;

/**
 * @author chanchal.chitwan on 06/04/17.
 */
public class KafkaOffsetsDaoException
        extends Exception {

    public KafkaOffsetsDaoException(String message) {
        super(message);
    }

    public KafkaOffsetsDaoException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaOffsetsDaoException(Throwable cause) {
        super(cause);
    }
}
