package com.kaede.esdemo.pojo;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * @author kaede
 * @create 2023-01-11
 */

@Data
@Document(indexName = "products", createIndex = true)
public class Product {

    @Id
    private Integer id;
    @Field(type = FieldType.Keyword)
    private String title;
    @Field(type = FieldType.Double)
    private Double price;
    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String description;

}
