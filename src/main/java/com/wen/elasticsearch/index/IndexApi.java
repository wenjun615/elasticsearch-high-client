package com.wen.elasticsearch.index;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * <p>
 * Index APIs
 * </p>
 *
 * @author wenjun
 * @since 2021/1/13
 */
@Component
public class IndexApi {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 创建索引
     *
     * @throws IOException
     */
    void createIndexRequest() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("twitter");
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
    }
}
