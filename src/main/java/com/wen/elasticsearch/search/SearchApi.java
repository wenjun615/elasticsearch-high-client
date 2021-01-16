package com.wen.elasticsearch.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Search APIs
 * </p>
 *
 * @author wenjun
 * @since 2021/1/13
 */
@Component
public class SearchApi {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 搜索全部
     *
     * @throws IOException
     */
    void searchRequest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("posts");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 匹配所有
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits.getHits()) {
            String index = hit.getIndex();
            String id = hit.getId();
            float score = hit.getScore();
            String sourceAsString = hit.getSourceAsString();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String documentTitle = (String) sourceAsMap.get("title");
            List<Object> users = (List<Object>) sourceAsMap.get("user");
            Map<String, Object> innerObject =
                    (Map<String, Object>) sourceAsMap.get("innerObject");
        }
    }

    /**
     * 分页
     */
    void searchRequest2() {
        SearchRequest searchRequest = new SearchRequest()
                .indices("posts");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        // 分页，默认 0
        sourceBuilder.from(0);
        // 默认 10
        sourceBuilder.size(5);
        // 超时时间
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
    }

    /**
     * 模糊匹配
     */
    void searchRequest3() {
        SearchRequest request = new SearchRequest()
                .indices("posts");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        /*MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("user", "kimchy")
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(3)
                .maxExpansions(10);*/
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("user", "kimchy")
                // 模糊匹配
                .fuzziness(Fuzziness.AUTO)
                // 设置前缀长度
                .prefixLength(3)
                // 设置最大扩展选项以控制查询的模糊过程
                .maxExpansions(10);
        sourceBuilder.query(matchQueryBuilder);
        request.source(sourceBuilder);
    }

    /**
     * 排序
     */
    void searchRequest4() {
        SearchRequest request = new SearchRequest()
                .indices("posts");
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("user", "kimchy")
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(3)
                .maxExpansions(10);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(matchQueryBuilder)
                // 默认分数 score 降序
                .sort(new ScoreSortBuilder().order(SortOrder.DESC))
                // 按 id 升序
                .sort(new FieldSortBuilder("id").order(SortOrder.ASC));
        request.source(sourceBuilder);
    }

    /**
     * 聚合
     *
     * @throws IOException
     */
    void requestingAggregations() throws IOException {
        SearchRequest request = new SearchRequest()
                .indices("posts");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 创建一个聚合操作，terms() 等价于 group by，表示桶的概念，by_company 是我们自定义的桶的名字
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_company")
                // group by company.keyword
                .field("company.keyword")
                // 指标，average_age 是我们自定义的指标的名字，这里是获取 age 字段的平均值
                .subAggregation(AggregationBuilders.avg("average_age").field("age"));
        searchSourceBuilder.aggregation(aggregation);
        // 执行搜索后的 SearchResponse 将包含特定的分析结果
        searchSourceBuilder.profile(true);
        request.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        Terms byCompanyAggregation = aggregations.get("by_company");
        Terms.Bucket elasticBucket = byCompanyAggregation.getBucketByKey("Elastic");
        Avg averageAge = elasticBucket.getAggregations().get("average_age");
        double avg = averageAge.getValue();
    }

    /**
     * 搜索补全
     */
    void requestingSuggestions() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestionBuilder termSuggestionBuilder =
                SuggestBuilders.termSuggestion("user").text("kmichy");
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);
    }
}
