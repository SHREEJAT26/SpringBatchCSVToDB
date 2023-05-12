package com.batch.SpringBatchCSVToDB.Entity;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter

public class Products
{
    private Integer id;
    private String name;
    private String description;
    private Double price;
}
