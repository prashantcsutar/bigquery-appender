package io.github.prashantcsutar.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.status.ErrorStatus;
import io.github.prashantcsutar.appender.api.BigQueryAppenderApi;
import io.github.prashantcsutar.appender.conf.BigQueryAppenderConfigurations;
import lombok.Getter;
import lombok.Setter;

/**
 * Google BigQuery Appender
 *
 * @author   Prashant Sutar
 * @since    1.0
 * @category LogbackAppender
 */
@Getter
@Setter
public class BigQueryAppender extends AppenderBase<ILoggingEvent> {
    private Encoder<ILoggingEvent> encoder;
    private BigQueryAppenderConfigurations configurations;

    /**
     * Append logs to Google BigQuery table
     *
     * @param eventObject
     */
    @Override
    protected void append(ILoggingEvent eventObject) {
        BigQueryAppenderApi.append(eventObject, encoder, configurations);
    }

    /**
     * Checks that requires parameters are set and if everything is in order,
     * activates this appender.
     */
    @Override
    public void start() {
        int errors = 0;

        if (this.configurations == null) {
            addStatus(new ErrorStatus("No configurations set for the appender named \"" + name + "\".", this));
            errors++;
        }

        if (errors == 0) {
            super.start();
        }
    }
}
