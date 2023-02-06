package com.kaede.esdemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kaede
 * @create 2023-01-11
 */

@SpringBootTest
public class RestHighLevelClientTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    // 创建索引映射
    @Test
    public void testCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("products");
        // 指定JSON结构(在Kibana中书写好后拿过来)和数据类型
        createIndexRequest.mapping("{\n" +
            "    \"properties\": {\n" +
            "      \"title\": {\"type\": \"keyword\"},\n" +
            "      \"price\": {\"type\": \"double\"},\n" +
            "      \"created_at\": {\"type\": \"date\"},\n" +
            "      \"description\": {\n" +
            "        \"type\": \"text\",\n" +
            "        \"analyzer\": \"ik_max_word\"\n" +
            "      }\n" +
            "    }\n" +
            "  }", XContentType.JSON);
        // 参数为创建索引的请求对象和请求的配置对象
        CreateIndexResponse createIndexResponse =
            restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.isAcknowledged());
        // 关闭资源
        restHighLevelClient.close();
    }

    // 删除索引
    @Test
    public void testDeleteIndex() throws IOException {
        // 参数为删除的索引和请求的配置对象
        AcknowledgedResponse acknowledgedResponse =
            restHighLevelClient.indices().delete(new DeleteIndexRequest("products"), RequestOptions.DEFAULT);
        System.out.println(acknowledgedResponse.isAcknowledged());
    }

    // 再次创建索引映射后
    // 索引文档
    @Test
    public void testCreate() throws IOException {
        IndexRequest indexRequest = new IndexRequest("products");
        // 指定文档id和数据
        indexRequest.id("1").source("{\n" +
            "  \"title\": \"小浣熊\",\n" +
            "  \"price\": 1.5,\n" +
            "  \"created_at\": \"2021-12-12\",\n" +
            "  \"description\": \"小浣熊饼干非常好吃\"\n" +
            "}", XContentType.JSON);
        // 参数为索引的请求对象和请求的配置对象
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.status());
    }

    // 更新文档
    @Test
    public void testUpdate() throws IOException {
        // 参数为更新的索引名和文档id
        UpdateRequest updateRequest = new UpdateRequest("products", "1");
        // 保留原始数据的基础上更新
        updateRequest.doc("{\n" +
            "  \"doc\": {\n" +
            "    \"title\": \"小浣熊干脆面\",\n" +
            "    \"price\": 2\n" +
            "  }\n" +
            "}", XContentType.JSON);
        // 参数为更新的请求对象和请求的配置对象
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
    }

    // 删除文档
    @Test
    public void testDelete() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("products", "1");
        // 参数为删除的请求对象和请求的配置对象
        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    // 再次添加文档后
    // 基于id查询文档
    @Test
    public void testQueryById() throws IOException {
        GetRequest getRequest = new GetRequest("products", "1");
        // 参数为查询的请求对象和请求的配置对象，返回值为查询的响应对象
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("id = " + getResponse.getId());
        System.out.println(getResponse.getSource());
        System.out.println(getResponse.getSourceAsString());
    }

    // 查询所有
    @Test
    public void testMatchAll() throws IOException {
        // 指定搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        // 指定的条件对象
        // SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询所有(只需改变条件即可实现各种查询)
        // sourceBuilder.query(QueryBuilders.matchAllQuery());
        // searchRequest.source(sourceBuilder);
        searchRequest.source().query(QueryBuilders.matchAllQuery());
        // 参数为搜索的请求对象和请求的配置对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 获取总条数
        System.out.println("总条数: " + searchResponse.getHits().getTotalHits().value);
        // 获取最大得分
        System.out.println("最大得分: " + searchResponse.getHits().getMaxScore());
        // 获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getId() + " " + hit.getSourceAsString());
        }
    }

    // 关键词查询
    @Test
    public void testTerm() throws IOException {
        // 指定搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        // 指定的条件对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 只需改变条件即可实现各种查询
        sourceBuilder.query(QueryBuilders.termQuery("description", "浣熊"));
        searchRequest.source(sourceBuilder);
        // 参数为搜索的请求对象和请求的配置对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 获取总条数
        System.out.println("总条数: " + searchResponse.getHits().getTotalHits().value);
        // 获取最大得分
        System.out.println("最大得分: " + searchResponse.getHits().getMaxScore());
        // 获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getId() + " " + hit.getSourceAsString());
        }
    }

    // 封装查询方法
    public void query(QueryBuilder queryBuilder) throws IOException {
        // 指定搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        searchRequest.source().query(queryBuilder);
        // 参数为搜索的请求对象和请求的配置对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 获取总条数
        System.out.println("总条数: " + searchResponse.getHits().getTotalHits().value);
        // 获取最大得分
        System.out.println("最大得分: " + searchResponse.getHits().getMaxScore());
        // 获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getId() + " " + hit.getSourceAsString());
        }
    }

    // 其他查询
    @Test
    public void testRange() throws IOException {
        // 范围查询
        query(QueryBuilders.rangeQuery("price").gte(1).lt(2));
        // 前缀查询
        query(QueryBuilders.prefixQuery("title", "小"));
        // 通配符查询
        query(QueryBuilders.wildcardQuery("title", "小?熊*"));
        // 多个指定id查询
        query(QueryBuilders.idsQuery().addIds("1").addIds("2"));
        // 多字段查询
        query(QueryBuilders.multiMatchQuery("非常不错", "title", "description"));
    }

    // 分页查询和排序
    @Test
    public void testPage() throws IOException {
        // 指定搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        searchRequest.source().query(QueryBuilders.matchAllQuery())
            // 默认返回10条，降序排序
            .from(0)    // 指定起始位置
            .size(2)    // 指定每页展示记录数
            .sort("price", SortOrder.ASC)   // 指定排序的字段和规则
            //参数1为包含字段的数组，参数2为排除字段的数组
            .fetchSource(new String[]{"title", "description"}, new String[]{});
        // 参数为搜索的请求对象和请求的配置对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数: " + searchResponse.getHits().getTotalHits().value);
        System.out.println("最大得分: " + searchResponse.getHits().getMaxScore());
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getId() + " " + hit.getSourceAsString());
        }
    }

    // 高亮查询
    @Test
    public void testHighlight() throws IOException {
        // 指定搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        // 创建高亮器
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false).field("*")
            .preTags("<span style=\"color:red\">")
            .postTags("</span>");
        searchRequest.source().query(QueryBuilders.termQuery("description", "小浣熊"))
            .fetchSource(new String[]{"title", "description"}, new String[]{})
            .highlighter(highlightBuilder);  // 高亮搜索结果
        // 参数为搜索的请求对象和请求的配置对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数: " + searchResponse.getHits().getTotalHits().value);
        System.out.println("最大得分: " + searchResponse.getHits().getMaxScore());
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getId() + " " + hit.getSourceAsString());
            // 获取高亮字段
            Map<String, HighlightField> fields = hit.getHighlightFields();
            fields.forEach((fieldName, result) -> {
                System.out.println(fieldName + ": " + result.getFragments()[0]);
            });
        }
    }

    // 过滤查询
    @Test
    public void testFilterQuery() throws IOException {
        // 指定搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        searchRequest.source().query(QueryBuilders.matchAllQuery())
            // 指定过滤条件
            .postFilter(QueryBuilders.rangeQuery("price").gt(1).lte(2))
            .postFilter(QueryBuilders.existsQuery("title"));
        // 参数为搜索的请求对象和请求的配置对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数: " + searchResponse.getHits().getTotalHits().value);
        System.out.println("最大得分: " + searchResponse.getHits().getMaxScore());
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getId() + " " + hit.getSourceAsString());
        }
    }

    // 将对象序列化到ES中
    @Test
    public void testIndex() throws IOException {
        Product product = new Product();
        product.setId(1);
        product.setTitle("小浣熊");
        product.setPrice(1.5);
        product.setDescription("小浣熊干脆面真好吃");
        // 录入ES中
        IndexRequest indexRequest = new IndexRequest("products");
        indexRequest.id(product.getId().toString())
            .source(new ObjectMapper().writeValueAsString(product), XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.status());
    }

    // 将文档反序列化为对象
    @Test
    public void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        searchRequest.source().query(QueryBuilders.matchAllQuery());
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数: " + searchResponse.getHits().getTotalHits().value);
        System.out.println("最大得分: " + searchResponse.getHits().getMaxScore());
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Product> productList = new ArrayList<>();
        for (SearchHit hit : hits) {
            Product product = new ObjectMapper().readValue(hit.getSourceAsString(), Product.class);
            productList.add(product);
        }
        productList.forEach((product -> System.out.println(product)));
    }

    @Test
    public void testSearch2() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false).field("title").field("description")
            .preTags("<span style=\"color:red\">").postTags("</span>");
        searchRequest.source().query(QueryBuilders.termQuery("description", "小浣熊"))
            .fetchSource(new String[]{}, new String[]{"price"})
            .highlighter(highlightBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("总条数: " + searchResponse.getHits().getTotalHits().value);
        System.out.println("最大得分: " + searchResponse.getHits().getMaxScore());
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Product> productList = new ArrayList<>();
        for (SearchHit hit : hits) {
            Product product = new ObjectMapper().readValue(hit.getSourceAsString(), Product.class);
            // 处理高亮
            Map<String, HighlightField> fields = hit.getHighlightFields();
            if (fields.containsKey("title")) {
                Text title = fields.get("title").fragments()[0];
                product.setTitle(title.toString());
            }
            if (fields.containsKey("description")) {
                Text description = fields.get("description").fragments()[0];
                product.setDescription(description.toString());
            }
            productList.add(product);
        }
        productList.forEach((product -> System.out.println(product)));
    }

    // 根据某个字段分组，统计数量
    @Test   
    public void testAggsGroup() throws IOException {
        SearchRequest searchRequest = new SearchRequest("fruit");
        searchRequest.source().size(0).aggregation(
            AggregationBuilders.terms("price_group").field("price").size(10))
            // 聚合时若不添加查询条件，默认查询所有
            .query(QueryBuilders.matchAllQuery());
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 处理聚合的结果
        Aggregations aggregations = searchResponse.getAggregations();
        // 根据名称获取聚合结果
        Terms priceGroup = aggregations.get("price_group");
        List<? extends Terms.Bucket> buckets = priceGroup.getBuckets();
        buckets.forEach(bucket -> {
            System.out.println(bucket.getKey() + ": " + bucket.getDocCount());
        });
    }

    // 求最大值
    @Test
    public void testAggsMax() throws IOException {
        SearchRequest searchRequest = new SearchRequest("fruit");
        searchRequest.source().aggregation(AggregationBuilders.max("price_max").field("price"));
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 处理聚合的结果
        Aggregations aggregations = searchResponse.getAggregations();
        // 根据不同的聚合返回值类型不同，对应为ParsedMax、ParsedAvg、ParsedSum等
        ParsedMax priceMax = aggregations.get("price_max");
        System.out.println(priceMax.getValue());
    }

}
