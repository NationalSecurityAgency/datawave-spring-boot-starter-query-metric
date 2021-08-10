package datawave.microservice.querymetric.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.security.authorization.JWTTokenHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "datawave.query.metric.client.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties({QueryMetricClientProperties.class})
@EnableBinding({QueryMetricSinkConfiguration.QueryMetricSinkBinding.class, QueryMetricSourceConfiguration.QueryMetricSourceBinding.class})
public class QueryMetricClientConfiguration {
    
    // A JWTTokenHandler is only necessary for HTTP and HTTPS transportTypes
    // and should not cause an autowire failure if it is not present
    @Autowired(required = false)
    private JWTTokenHandler jwtTokenHandler;
    
    @Bean
    @ConditionalOnMissingBean
    QueryMetricClient queryMetricClient(RestTemplateBuilder restTemplateBuilder, QueryMetricClientProperties queryMetricClientProperties,
                    QueryMetricSourceConfiguration.QueryMetricSourceBinding queryMetricSourceBinding, ObjectMapper objectMapper) {
        QueryMetricClient queryMetricClient = new QueryMetricClient(restTemplateBuilder, queryMetricClientProperties, queryMetricSourceBinding, objectMapper);
        if (this.jwtTokenHandler != null) {
            queryMetricClient.setJwtTokenHandler(this.jwtTokenHandler);
        }
        return queryMetricClient;
    }
}
