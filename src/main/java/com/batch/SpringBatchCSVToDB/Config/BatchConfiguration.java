package com.batch.SpringBatchCSVToDB.Config;

import com.batch.SpringBatchCSVToDB.Entity.Products;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.PathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfiguration
{
    @Autowired
    DataSource dataSource;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Bean
    public Job job() {
        return new JobBuilder("Job-1", jobRepository).flow(step()).end().build();

    }

    @Bean
    public Step step()
    {
        StepBuilder stepBuilder = new StepBuilder("Step-1",jobRepository);
        return stepBuilder.<Products,Products>chunk(4,transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer()).build();
    }


    @Bean
    public ItemReader<Products> reader()
    {
        FlatFileItemReader<Products> reader = new FlatFileItemReader<>();
        reader.setResource(new PathResource("C:\\Users\\shrthudh\\IdeaProjects\\SpringBatchCSVToDB\\src\\main\\resources\\Products.csv"));

        DefaultLineMapper<Products> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setNames("id","name","description","price");

        BeanWrapperFieldSetMapper<Products> fieldSetMapper = new BeanWrapperFieldSetMapper<Products>();

        fieldSetMapper.setTargetType(Products.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        reader.setLineMapper(lineMapper);
        return reader;
    }


    @Bean
    public ItemProcessor<Products,Products> processor()
    {
        return (p)->{
            p.setPrice(p.getPrice()-p.getPrice()*10/100);
            return p;
        };

    }


    @Bean
    public ItemWriter<Products> writer()
    {
        JdbcBatchItemWriter<Products> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Products>());
        writer.setSql("INSERT INTO PRODUCTS(ID,NAME,DESCRIPTION,PRICE) VALUES(:id,:name,:description,:price)");
        return writer;

    }

}
