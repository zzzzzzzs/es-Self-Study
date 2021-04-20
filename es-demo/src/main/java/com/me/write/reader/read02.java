package com.me.write.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class read02 {
    public static void main(String[] args) throws IOException {
        //1.创建jest客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.读取数据
        //TODO 4.1编辑查询语句
        //TODO 相当于查询语句最外成的{}
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//----------------------------------querry------------------------------------
        //TODO 相当于"bool"
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //TODO 相当于"term"
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex","男");
        //TODO 相当于"filter"
        boolQueryBuilder.filter(termQueryBuilder);
        //TODO 相当于 "query"
        sourceBuilder.query(boolQueryBuilder);
        //TODO 相当于"match"
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo","球");
        //TODO 相当于"must"
        boolQueryBuilder.must(matchQueryBuilder);
//----------------------------------agg------------------------------------
        //TODO 相当于"terms"
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupByClass").field("class_id").size(10);
        //TODO 相当于"max"
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("groupByAge").field("age");
        //TODO 相当于"aggs"
        sourceBuilder.aggregation(termsAggregationBuilder.subAggregation(maxAggregationBuilder));
//----------------------------------from------------------------------------
        //TODO 相当于"from"
        sourceBuilder.from(0);
//----------------------------------size------------------------------------
        //TODO 相当于"size"
        sourceBuilder.size(2);

        Search search = new Search.Builder(sourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();
        SearchResult result = jestClient.execute(search);
        //5.解析数据
        //TODO 获取命中数据条数
        System.out.println("命中"+result.getTotal()+"条数据");

        //TODO 获取具体数据
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index:"+hit.index);
            System.out.println("_type:"+hit.type);
            System.out.println("_id:"+hit.id);
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o+":"+source.get(o));
            }
        }

        //TODO 获取聚合数据
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
        //TODO 解析班级聚合组数据
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:"+bucket.getKey());
            System.out.println("doc_count:"+bucket.getCount());
            //TODO 解析嵌套的年龄聚合组数据
            MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
            System.out.println("value:"+groupByAge.getMax());
        }

        //关闭连接
        jestClient.shutdownClient();
    }
}
