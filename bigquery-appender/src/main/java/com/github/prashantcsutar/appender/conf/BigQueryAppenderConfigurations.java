package com.github.prashantcsutar.appender.conf;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * BigQuery appender configurations
 *
 * @author Prashant Sutar
 * @since  1.0
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
public class BigQueryAppenderConfigurations {
    private String credentialsPath;
    private String projectId;
    private String location;
    private String datasetId;
    private String tableId;
    private String messageKey = "message";
}
