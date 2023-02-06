package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static cn.itcast.hotel.constant.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
class HotelDemoApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;
    @Autowired
    private IHotelService hotelService;
    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void createHotelIndex () throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("hotel");
        // 指定JSON结构(在Kibana中书写好后拿过来)和数据类型
        createIndexRequest.source(MAPPING_TEMPLATE , XContentType.JSON);
        // 参数为创建索引的请求对象和请求的配置对象
        CreateIndexResponse createIndexResponse =
            restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.isAcknowledged());
        // 关闭资源
        restHighLevelClient.close();
    }

    @Test
    public void testBulk() throws IOException {
        // 批量查询酒店数据
        List<Hotel> hotels = hotelService.list();
        // 1.创建Bulk请求
        BulkRequest request = new BulkRequest();
        // 2.添加要批量提交的请求
        // 转换为文档类型，HotelDoc
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            // 创建新增文档的Request对象
            request.add(new IndexRequest("hotel").id(hotelDoc.getId().toString())
                .source(mapper.writeValueAsString(hotelDoc), XContentType.JSON));
        }
        // 3.发起Bulk请求
        BulkResponse response = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        BulkItemResponse[] items = response.getItems();
        for (BulkItemResponse item : items) {
            System.out.println(item.getResponse().getResult().toString() + item.getResponse().status().toString());
        }
    }

    @Test
    public void testSuggest() {
        try {
            // 1.准备request
            SearchRequest request = new SearchRequest("hotel");
            // 2.准备DSL
            request.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions", SuggestBuilders.completionSuggestion("suggestion")
                    .prefix("h").skipDuplicates(true).size(10)
            ));
            // 3.发起请求
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            // 4.解析结果
            // 4.1.根据名称获取补全结果
            CompletionSuggestion suggestions = response.getSuggest().getSuggestion("suggestions");
            // 4.2.获取options进行遍历
            for (CompletionSuggestion.Entry.Option option : suggestions.getOptions()) {
                // 4.3.获取option中的text，即补全的词条
                System.out.println(option.getText().toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
