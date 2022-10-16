package com.toob.bootcdc.config;

import com.toob.bootcdc.props.SourceDatabaseProps;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.File;

import static java.util.Objects.requireNonNull;

/**
 * @author: Thabo Lebogang Matjuda
 * @since: 2022-10-12
 */

@Configuration
public class CdcConfig {

    private final SourceDatabaseProps sourceDatabaseProps;

    private final Environment env;


    public CdcConfig( SourceDatabaseProps sourceDatabaseProps, Environment env) {
        this.sourceDatabaseProps = sourceDatabaseProps;
        this.env = env;
    }


    @Bean
    public io.debezium.config.Configuration customerConnector() {
        return io.debezium.config.Configuration.create()
                .with("name", "microsoft-sql-server-connector")
                .with("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", new File( requireNonNull( env.getProperty("cdc.debezium.offset.file.path"))))
                .with("offset.flush.interval.ms", "60000")
                .with("database.hostname", sourceDatabaseProps.getHost())
                .with("database.port", sourceDatabaseProps.getPort())
                .with("database.user", sourceDatabaseProps.getUsername())
                .with("database.password", sourceDatabaseProps.getPassword())
                .with("database.dbname", "ChangeDataCaptureBoot")
                .with("database.include.list", "ChangeDataCaptureBoot")
                .with("table.include.list", "dbo.Person")
                .with("include.schema.changes", "false")
                .with("database.server.id", "10181")
                .with("database.server.name", "microsoft-sql-server-instance")
                .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename", new File( requireNonNull( env.getProperty("cdc.debezium.databaseHistory.file.path"))))
                .with("database.encrypt", "true") // SQL Server Specific
                .with("database.trustServerCertificate", "true") // SQL Server Specific
                .build();
    }

}
