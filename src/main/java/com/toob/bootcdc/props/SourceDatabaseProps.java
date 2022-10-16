package com.toob.bootcdc.props;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author: Thabo Lebogang Matjuda
 * @since: 2022-10-12
 */

@Component
@ConfigurationProperties( prefix = "cdc.source.database")
@Getter
@Setter
public class SourceDatabaseProps {

    private String host;
    private String username;
    private String password;
    private String port;

}
