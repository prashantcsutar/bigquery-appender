package io.github.prashantcsutar.appender.api;

/**
 * BiqQueryAppender exception
 *
 * @author Prashant Sutar
 * @since  1.0
 */
public class BigQueryAppenderException extends RuntimeException {

    private static final long serialVersionUID = -7877090010017872271L;

    public BigQueryAppenderException(String message, Throwable cause) {
        super(message, cause);
    }

    public BigQueryAppenderException(String message) {
        super(message);
    }
}
