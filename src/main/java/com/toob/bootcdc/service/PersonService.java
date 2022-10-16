package com.toob.bootcdc.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Service;

import java.util.Map;

import static io.debezium.data.Envelope.Operation;

/**
 * @author: Thabo Lebogang Matjuda
 * @since: 2022-10-12
 */

@Slf4j
@Service
public class PersonService {

    /**
     * Do anything with the data here.
     * @param customerData
     * @param operation
     */
    public void processData( Map<String, Object> customerData, Operation operation) {
        log.info("We have just gotten latest info from the database for operation : {}", operation.name());
        if ( MapUtils.isNotEmpty( customerData)) {
            customerData.forEach( (k, v) -> log.info("The property : {} has a value of : {}", k, v.toString()));
        }

        log.info("Bye Bye!");
    }

}
