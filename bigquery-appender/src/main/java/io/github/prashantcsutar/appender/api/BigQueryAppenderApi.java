package io.github.prashantcsutar.appender.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.slf4j.LoggerFactory;

import com.google.api.client.util.DateTime;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.BigQueryOptions.Builder;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.encoder.Encoder;
import io.github.prashantcsutar.appender.conf.BigQueryAppenderConfigurations;

/**
 * BigQuery APIs
 *
 * @author Prashant Sutar
 * @since  1.0
 */
public class BigQueryAppenderApi {
    private static final Logger logger = (Logger) LoggerFactory.getLogger(BigQueryAppenderApi.class);

    private static BigQuery bigQuery = null;

    private BigQueryAppenderApi() {
    }

    /**
     * Initializes instance of BigQuery API to insert logs into BigQuery table
     *
     * @param configurations BigQuery configurations
     *
     * @return BigQuery instance
     *
     * @throws BigQueryAppenderException
     */
    private static BigQuery bigQueryClient(BigQueryAppenderConfigurations configurations) {
        try {
            if (Objects.isNull(bigQuery)) {
                logger.debug("BigQuery configurations: {}", configurations);
                File credentialsPath = new File(configurations.getCredentialsPath());
                GoogleCredentials credentials;
                try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
                    credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
                }

                Builder builder = BigQueryOptions.newBuilder();
                builder.setProjectId(configurations.getProjectId()).setCredentials(credentials);

                if (!"default".equalsIgnoreCase(configurations.getLocation())) {
                    builder.setLocation(configurations.getLocation());
                }
                bigQuery = builder.build().getService();
            }

            return bigQuery;
        } catch (IOException ioException) {
            throw new BigQueryAppenderException(ioException.getMessage(), ioException);
        }
    }

    /**
     * Insert logs into Google BigQuery table
     *
     * @param event          Logging event
     * @param encoder        Encoder
     * @param configurations BigQuery appender configurations
     *
     * @throws BigQueryAppenderException
     */
    public static void append(ILoggingEvent event, Encoder<ILoggingEvent> encoder,
            BigQueryAppenderConfigurations configurations) {
        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put(BigQueryAppenderConstant.TIMESTAMP, new DateTime(new Date()));
        String message = Objects.nonNull(encoder) ? new String(encoder.encode(event)) : event.getFormattedMessage();
        rowContent.put(configurations.getMessageKey(), message);

        insertRowsIntoBigQueryTable(rowContent, configurations);
    }

    /**
     * Insert rows into BigQuery table
     *
     * @param rowContent     Row data
     * @param configurations BigQuery Configurations
     *
     * @throws BigQueryAppenderException
     */
    private static void insertRowsIntoBigQueryTable(Map<String, Object> rowContent,
            BigQueryAppenderConfigurations configurations) {
        try {
            TableId tableIdObj = TableId.of(configurations.getDatasetId(), configurations.getTableId());

            InsertAllResponse response = bigQueryClient(configurations)
                    .insertAll(InsertAllRequest.newBuilder(tableIdObj).addRow(rowContent).build());

            if (response.hasErrors()) {
                logger.error("Unable to insert logs to BigQuery {}", response.getInsertErrors());
                throw new BigQueryAppenderException(response.getInsertErrors().toString());
            }
        } catch (BigQueryException e) {
            logger.error(e.getMessage(), e);
            throw new BigQueryAppenderException(e.getMessage(), e.getCause());
        }
    }
}
