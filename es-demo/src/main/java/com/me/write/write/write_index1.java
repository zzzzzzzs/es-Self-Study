package com.me.write.write;

import com.me.write.bean.Movie2;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class write_index1 {
    public static void main(String[] args) throws IOException {
        //1.获取jest客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接ES地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.操作数据，往ES中写入数据
        Index index = new Index.Builder("{\n" +
                "  \"id\":\"1003\",\n" +
                "  \"movie_name\":\"一路向北\"\n" +
                "}").index("movie2").type("_doc").id("1003").build();
        jestClient.execute(index);


        //最后一步：关闭连接
        jestClient.shutdownClient();
    }
}
