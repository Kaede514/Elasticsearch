package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public PageResult search(RequestParams params) {
        try {
            // 1.??????request
            SearchRequest searchRequest = new SearchRequest("hotel");
            // 2.??????DSL
            // 2.1.query
            buildBasicQuery(params, searchRequest);
            // 2.2.??????
            int page = params.getPage();
            int size = params.getSize();
            searchRequest.source().from((page - 1) * size).size(size);
            // 2.3.??????
            String location = params.getLocation();
            if (!StringUtils.isEmpty(location)) {
                searchRequest.source().sort(
                    SortBuilders.geoDistanceSort("location", new GeoPoint(location)
                ).order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS));
            }
            // 3.???????????????????????????
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 4.????????????
            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        try {
            // 1.??????request
            SearchRequest searchRequest = new SearchRequest("hotel");
            // 2.??????DSL
            // 2.1.query
            buildBasicQuery(params, searchRequest);
            searchRequest.source().size(0);
            // 2.2.??????
            buildAggregation(searchRequest);
            // 3.????????????
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            // 4.????????????
            // ?????????????????????
            Map<String, List<String>> resultMap = new HashMap<>();
            Aggregations aggregations = searchResponse.getAggregations();
            // ??????????????????????????????
            List<String> brandList = getAggByName(aggregations, "brandAgg");
            List<String> cityList = getAggByName(aggregations, "cityAgg");
            List<String> starNameList = getAggByName(aggregations, "starNameAgg");
            resultMap.put("brand", brandList);
            resultMap.put("city", cityList);
            resultMap.put("starName", starNameList);
            return resultMap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestions(String key) {
        try {
            // 1.??????request
            SearchRequest request = new SearchRequest("hotel");
            // 2.??????DSL
            request.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions", SuggestBuilders.completionSuggestion("suggestion")
                    .prefix(key).skipDuplicates(true).size(10)
            ));
            // 3.????????????
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            // 4.????????????
            // 4.1.??????????????????????????????
            CompletionSuggestion suggestions = response.getSuggest().getSuggestion("suggestions");
            // 4.2.??????options????????????
            List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
            List<String> list = new ArrayList<>(options.size());
            for (CompletionSuggestion.Entry.Option option : options) {
                // 4.3.??????option??????text?????????????????????
                list.add(option.getText().toString());
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertById(Long id) {
        try {
            Hotel hotel = getById(id);
            HotelDoc hotelDoc = new HotelDoc(hotel);
            // 1.??????request
            IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
            // 2.??????DSL
            ObjectMapper mapper = new ObjectMapper();
            request.source(mapper.writeValueAsString(hotelDoc), XContentType.JSON);
            // 3.????????????
            restHighLevelClient.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            // 1.??????request
            DeleteRequest request = new DeleteRequest("hotel", id.toString());
            // 2.????????????
            restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getAggByName(Aggregations aggregations, String name) {
        Terms brandAgg = aggregations.get(name);
        List<? extends Terms.Bucket> brandBuckets = brandAgg.getBuckets();
        List<String> brandList = new ArrayList<>(brandBuckets.size());
        brandBuckets.forEach(bucket -> {
            String key = bucket.getKeyAsString();
            brandList.add(key);
        });
        return brandList;
    }

    private void buildAggregation(SearchRequest searchRequest) {
        searchRequest.source().aggregation(
            AggregationBuilders.terms("brandAgg").field("brand").size(10));
        searchRequest.source().aggregation(
            AggregationBuilders.terms("cityAgg").field("city").size(10));
        searchRequest.source().aggregation(
            AggregationBuilders.terms("starNameAgg").field("starName").size(10));
    }

    private void buildBasicQuery(RequestParams params, SearchRequest request) {
        // 2.1.??????BooleanQuery
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        // 2.2.???????????????
        String key = params.getKey();
        if (StringUtils.isEmpty(key)) {
            query.must(QueryBuilders.matchAllQuery());
        } else {
            query.must(QueryBuilders.matchQuery("all", key));
        }
        // 2.3.????????????
        if (!StringUtils.isEmpty(params.getCity())) {
            query.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        if (!StringUtils.isEmpty(params.getBrand())) {
            query.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        if (!StringUtils.isEmpty(params.getStarName())) {
            query.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        if (params.getMinPrice() != null) {
            query.filter(QueryBuilders.rangeQuery("price").gte(params.getMinPrice()));
        }
        if (params.getMaxPrice() != null) {
            query.filter(QueryBuilders.rangeQuery("price").lte(params.getMaxPrice()));
        }
        // 2.4.????????????
        FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(
            // ???????????????????????????????????????
            query,
            // function socre?????????
            new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                // ???????????????function score??????
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                    // ????????????
                    QueryBuilders.termQuery("isAD", true),
                    // ?????????????????????????????????????????????????????????10
                    ScoreFunctionBuilders.weightFactorFunction(10)
                )
            }
        );
        request.source().query(functionScoreQuery);
    }

    // ????????????
    private PageResult handleResponse(SearchResponse searchResponse) throws JsonProcessingException {
        SearchHit[] hits = searchResponse.getHits().getHits();
        long total = searchResponse.getHits().getTotalHits().value;
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            HotelDoc hotelDoc = new ObjectMapper().readValue(hit.getSourceAsString(), HotelDoc.class);
            // ???????????????????????????
            Object[] sortValues = hit.getSortValues();
            if (!ObjectUtils.isEmpty(sortValues)) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);
        }
        return new PageResult(total, hotels);
    }

}
