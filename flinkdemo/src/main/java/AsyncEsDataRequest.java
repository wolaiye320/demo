import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @Auther: jiashaoguan
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
public class AsyncEsDataRequest extends RichAsyncFunction<Tuple2<Tuple2<String, String>, Integer>, Tuple2<Tuple2<String, String>, String>> {

    private transient RestHighLevelClient restHighLevelClient;

    private transient volatile Cache<Tuple2<String, String>, String> cityPercent;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化ElasticSearch-Client
        //restHighLevelClient = CommonUtil.getRestHighLevelClient();
          restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));
        //缓存设置
        cityPercent = CacheBuilder.<Tuple2<String, String>, String> newBuilder().maximumSize(10).expireAfterWrite(5, TimeUnit.MINUTES)
                .removalListener(
                        //生成环境,可以注销,这个是测试观察缓存使用
                        new RemovalListener<Object, Object>() {
                            @Override
                            public void onRemoval(RemovalNotification<Object, Object> notification) {
                                System.out.println(notification.getKey() + " wa remove,cause is:" + notification.getCause());
                            }
                        }
                ).build();
    }

    @Override
    public void close() throws Exception {
        restHighLevelClient.close();
    }


    @Override
    public void asyncInvoke(Tuple2<Tuple2<String, String>, Integer> input, ResultFuture<Tuple2<Tuple2<String, String>, String>> resultFuture) throws Exception {
        Tuple2<String, String> fromToCity = input.f0;
        //若缓存里存在,直接从缓存里读取key
        String stationPercent = cityPercent.getIfPresent(fromToCity);
        if (stationPercent != null) {
            System.out.println("get data from the cache :" + stationPercent);
            resultFuture.complete(Collections.singleton(new Tuple2<Tuple2<String, String>, String>(input.f0, stationPercent)));
        } else {
            search(input, resultFuture);
        }

    }
    //异步去读Es表
    private void search(Tuple2<Tuple2<String, String>, Integer> input, ResultFuture<Tuple2<Tuple2<String, String>, String>> resultFuture) {
        SearchRequest searchRequest = new SearchRequest("posts");
        searchRequest.indices("trafficwisdom.train_section_percent");
        String fromCity = input.f0.f0;
        String toCity = input.f0.f1;
        QueryBuilder builder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("from_city", fromCity)).must(QueryBuilders.termQuery("to_city", toCity));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(builder);
        searchRequest.source(sourceBuilder);
        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
            //成功
            @Override
            public void onResponse(SearchResponse searchResponse) {
                String stationPercent = null;
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                if (searchHits.length > 0) {
                    JSONObject jsonObject = JSONObject.parseObject(searchHits[0].getSourceAsString());
                    stationPercent = jsonObject.getString("section_search_percent");
                    cityPercent.put(input.f0, stationPercent);
                }
                System.out.println("get data from the es :" + stationPercent);
                resultFuture.complete(Collections.singleton(new Tuple2<Tuple2<String, String>, String>(input.f0, stationPercent)));
            }

            //失败
            @Override
            public void onFailure(Exception e) {
                resultFuture.complete(Collections.singleton(new Tuple2<Tuple2<String, String>, String>(input.f0, null)));
            }
        };
        restHighLevelClient.searchAsync(searchRequest, listener);
    }
}
