package com.wen.elasticsearch.document;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * Document APIs
 * </p>
 *
 * @author wenjun
 * @since 2021/1/6
 */
@Component
public class DocumentApi {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 索引请求
     *
     * @throws IOException
     */
    void indexRequest() throws IOException {
        // 索引
        IndexRequest request = new IndexRequest("posts");
        // 文档ID
        request.id("1");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        // 文档内容
        request.source(jsonString, XContentType.JSON);
        // 同步执行得到响应
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
    }

    void indexRequest2() {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest indexRequest = new IndexRequest("posts")
                .id("1")
                .source(jsonMap);
    }

    void indexRequest3() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "kimchy");
                builder.timeField("postDate", new Date());
                builder.field("message", "trying out Elasticsearch");
            }
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest("posts")
                    .id("1")
                    .source(builder);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void indexRequest4() {
        IndexRequest indexRequest = new IndexRequest("posts")
                .id("1")
                .source("user", "kimchy",
                        "postDate", new Date(),
                        "message", "trying out Elasticsearch");
    }

    /**
     * 获取请求
     *
     * @throws IOException
     */
    void getRequest() throws IOException {
        GetRequest getRequest = new GetRequest("posts", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            // 将文档检索为String
            String sourceAsString = getResponse.getSourceAsString();
            // 将文档检索为Map<String, Object>
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            // 将文档检索为byte[]
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
        }
    }
}
