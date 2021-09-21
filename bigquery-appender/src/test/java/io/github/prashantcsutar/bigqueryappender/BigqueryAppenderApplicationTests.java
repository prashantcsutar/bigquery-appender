package io.github.prashantcsutar.bigqueryappender;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import io.github.prashantcsutar.appender.BigQueryAppender;
import io.github.prashantcsutar.appender.conf.BigQueryAppenderConfigurations;

class BigqueryAppenderTests {
    @Test
    void test() {
        final Logger logger = (Logger) LoggerFactory.getLogger(BigqueryAppenderTests.class);

        BigQueryAppender bigQueryAppender = Mockito.mock(BigQueryAppender.class);
        bigQueryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());

        BigQueryAppenderConfigurations configurations = new BigQueryAppenderConfigurations();
        configurations.setCredentialsPath("src/test/resources/credentials.json");
        configurations.setDatasetId("mydataset");
        configurations.setTableId("logs");
        configurations.setProjectId("myproject");
        configurations.setLocation("default");
        configurations.setMessageKey("message");
        bigQueryAppender.setConfigurations(configurations);

        logger.setLevel(Level.DEBUG);
        logger.addAppender(bigQueryAppender);

        bigQueryAppender.start();

        logger.debug("debug log");

        verify(bigQueryAppender, times(1)).doAppend(Mockito.any(ILoggingEvent.class));
    }
}
