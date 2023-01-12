package com.kaede.esdemo;

import com.kaede.esdemo.pojo.Product;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Query;

import java.io.IOException;

@SpringBootTest
class ElasticsearchOperationsTests {

    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    // 索引文档
    @Test
    public void testIndex() throws IOException {
        Product product = new Product();
        // 存在id指定id，不存在id自动生成id
        product.setId(1);
        product.setTitle("怡宝矿泉水");
        product.setPrice(129.11);
        product.setDescription("我们喜欢喝矿泉水....");
        // 添加或更新文档，文档id不存在为添加，存在为更新
        elasticsearchOperations.save(product);
    }

    // 查询文档
    @Test
    public void testGet() {
        // 通过文档id查询到指定文档并转换为Java对象
        Product product = elasticsearchOperations.get("1", Product.class);
        System.out.println(product);
    }

    // 删除文档
    @Test
    public void testDelete() {
        Product product = new Product();
        product.setId(1);
        //传入对象或id，建议传入对象
        String delete = elasticsearchOperations.delete(product);
        System.out.println(delete);
    }

    // 删除所有
    @Test
    public void testDeleteAll() {
        // Query.findAll()为查询所有，并传入类型
        elasticsearchOperations.delete(Query.findAll(), Product.class);
    }

    // 查询所有
    @Test
    public void testFindAll() throws IOException {
        testIndex();
        SearchHits<Product> productSearchHits = elasticsearchOperations.search(Query.findAll(), Product.class);
        System.out.println("总大分数: " + productSearchHits.getMaxScore());
        System.out.println("符合条件的总条数: " + productSearchHits.getTotalHits());
        productSearchHits.forEach(productSearchHit -> {
            System.out.println("id: " + productSearchHit.getId());
            System.out.println("score: " + productSearchHit.getScore());
            Product product = productSearchHit.getContent();
            System.out.println("product: " + product);
        });
    }

    // 更新文档

}
