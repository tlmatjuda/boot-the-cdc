package com.toob.bootcdc.listener;

import com.toob.bootcdc.service.PersonService;
import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.debezium.data.Envelope.FieldName.*;
import static io.debezium.data.Envelope.Operation;
import static java.util.stream.Collectors.toMap;

/**
 * @author: Thabo Lebogang Matjuda
 * @since: 2022-10-12
 */

@Slf4j
@Component
public class DebeziumListener {

    private final Executor executor = Executors.newSingleThreadExecutor();
    private final PersonService customerService;
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;

    public DebeziumListener( Configuration customerConnectorConfiguration, PersonService personService) {

        // Starting up the Debezium Engine and passing the Events to our Listener for processing.
        this.debeziumEngine = DebeziumEngine.create( ChangeEventFormat.of(Connect.class))
                .using( customerConnectorConfiguration.asProperties())
                .notifying( this::handleChangeEvent)
                .build();

        this.customerService = personService;
    }

    /**
     * This is where the Magic Happens.
     * This method is mainly used for Data Extraction from the Events.
     * So A Record will be Changes and we will want to extract that info in here.
     * @param sourceEventRecord
     */
    private void handleChangeEvent( RecordChangeEvent<SourceRecord> sourceEventRecord) {
        SourceRecord sourceRecord = sourceEventRecord.record();
        log.debug("Key = '" + sourceRecord.key() + "' value = '" + sourceRecord.value() + "'");
        Struct sourceRecordChangeValue= (Struct) sourceRecord.value();

        if ( Objects.nonNull( sourceRecordChangeValue)) {
            Operation operation = Operation.forCode((String) sourceRecordChangeValue.get(OPERATION));
            if( operation != Operation.READ) {
                String record = operation == Operation.DELETE ? BEFORE : AFTER; // Handling Update & Insert operations.

                // Special Record that comes with info from the database table.
                Struct struct = (Struct) sourceRecordChangeValue.get(record);

                // Let's extract the Columns as Keys and the Records values
                List<Field> tableFields = struct.schema().fields();
                Map<String, Object> payload = tableFields.stream()
                        .map( Field::name)
                        .filter( fieldName -> Objects.nonNull( struct.get(fieldName))) // Filter the fields that are not null
                        .map( fieldName -> Pair.of(fieldName, struct.get(fieldName))) // Now get a Key-Value pair of the FieldName & the FieldValue
                        .collect( toMap( Pair::getKey, Pair::getValue)); // Take the Pair and map it

                // Now we can pass it to anything we went.
                this.customerService.processData(payload, operation);
                log.info("Updated Data: {} with Operation: {}", payload, operation.name());
            }
        }
    }

    /**
     * Business Logic that takes place after this class has been constructed.
     */
    @PostConstruct
    private void start() {
        this.executor.execute(debeziumEngine);
    }

    /**
     * Business Logic that takes place after this class has been destructed and destroyed.
     * @throws IOException
     */
    @PreDestroy
    private void stop() throws IOException {
        if (this.debeziumEngine != null) {
            this.debeziumEngine.close();
        }
    }

}
