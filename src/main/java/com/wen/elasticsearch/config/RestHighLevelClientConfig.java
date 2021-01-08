package com.wen.elasticsearch.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <p>
 * RestHighLevelClientConfig
 * </p>
 *
 * @author wenjun
 * @since 2021/1/6
 */
@Configuration
public class RestHighLevelClientConfig {

    @Bean
    public RestHighLevelClient client() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
    }

    /*RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http")));

    void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
}
