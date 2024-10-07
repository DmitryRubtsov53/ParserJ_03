package dn.rubtsov.parserj_03.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "fields")
@Getter
@Setter
public class MappingConfiguration {
    private Map<String, String> fieldMappings;
}
