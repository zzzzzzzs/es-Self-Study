package com.me.write.write;

import com.me.write.bean.Movie2;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class write_Bulk {
    public static void main(String[] args) throws IOException {
        //1.创建jest客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.批量写入数据

        Movie2 movie3 = new Movie2("1003", "一路向西");
        Movie2 movie4 = new Movie2("1004", "一路向东");
        Movie2 movie5 = new Movie2("1005", "一路向南");
        Index index3 = new Index.Builder(movie3).id("1003").build();
        Index index4 = new Index.Builder(movie4).id("1004").build();
        Index index5 = new Index.Builder(movie5).id("1005").build();

        Bulk build = new Bulk.Builder()
                .defaultIndex("movie2")
                .defaultType("_doc")
                .addAction(index3)
                .addAction(index4)
                .addAction(index5)
                .build();

        jestClient.execute(build);

        //关闭连接
        jestClient.shutdownClient();


    }
}
