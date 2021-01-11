package com.wen.elasticsearch.document;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
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
        String jsonString = indexRequest1();
        // 文档内容
        request.source(jsonString, XContentType.JSON);
        // 同步执行得到响应
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
    }

    String indexRequest1() {
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        return jsonString;
    }

    Map<String, Object> indexRequest2() {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest indexRequest = new IndexRequest("posts")
                .id("1")
                .source(jsonMap);
        return jsonMap;
    }

    XContentBuilder indexRequest3() {
        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder();
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
        return builder;
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

    /**
     * 获取源请求
     *
     * @throws IOException
     */
    public void getSourceRequest() throws IOException {
        GetSourceRequest getSourceRequest = new GetSourceRequest("posts", "1");
        // 同步执行
        GetSourceResponse response = client.getSource(getSourceRequest, RequestOptions.DEFAULT);
        // 获取源数据
        Map<String, Object> source = response.getSource();
    }

    /**
     * 存在请求
     *
     * @throws IOException
     */
    public void getRequestExists() throws IOException {
        GetRequest getRequest = new GetRequest("posts", "1");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
    }

    /**
     * 删除请求
     *
     * @throws IOException
     */
    public void deleteRequest() throws IOException {
        DeleteRequest request = new DeleteRequest("posts", "1");
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 更新请求
     *
     * @throws IOException
     */
    public void updateRequest() throws IOException {
        // 部分更新
        UpdateRequest request = new UpdateRequest("posts", "1")
                .doc("updated", new Date(), "reason", "daily update");
        // 如果文档尚不存在，可以将下面内容作为新文档插入
        String jsonString = "{\"created\":\"2017-01-01\"}";
        request.upsert(jsonString, XContentType.JSON);
        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
    }

    /**
     * 批量请求
     *
     * @throws IOException
     */
    public void bulkRequest() throws IOException {
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("posts").id("1")
                .source(XContentType.JSON, "field", "foo"));
        request.add(new IndexRequest("posts").id("2")
                .source(XContentType.JSON, "field", "bar"));
        request.add(new IndexRequest("posts").id("3")
                .source(XContentType.JSON, "field", "baz"));
        request.add(new DeleteRequest("posts", "3"));
        request.add(new UpdateRequest("posts", "2")
                .doc(XContentType.JSON, "other", "test"));
        request.add(new IndexRequest("posts").id("4")
                .source(XContentType.JSON, "field", "baz"));
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
    }

    /**
     * 批量获取请求
     *
     * @throws IOException
     */
    public void multiGetRequest() throws IOException {
        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item("index", "example_id"));
        request.add(new MultiGetRequest.Item("index", "another_id"));
        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
    }

    /**
     * 通过查询请求更新
     *
     * @throws IOException
     */
    public void updateByQueryRequest() throws IOException {
        // 在原索引基础上建立新索引
        UpdateByQueryRequest request = new UpdateByQueryRequest("source1", "source2");
        // 仅复制字段 user=kimchy 的
        request.setQuery(new TermQueryBuilder("user", "kimchy"));
        BulkByScrollResponse bulkResponse = client.updateByQuery(request, RequestOptions.DEFAULT);
    }
}
