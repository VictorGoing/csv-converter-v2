package com.kodilla.csvconverterv2;

import com.kodilla.csvconverterv2.domain.User;
import com.kodilla.csvconverterv2.domain.UserDto;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public BatchConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    FlatFileItemReader<UserDto> reader() {
        FlatFileItemReader<UserDto> reader = new FlatFileItemReader<>();
        reader().setResource(new ClassPathResource("input.csv"));

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("firstName", "lastName", "dateOfBirth");

        BeanWrapperFieldSetMapper<UserDto> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(UserDto.class);

        DefaultLineMapper<UserDto> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mapper);

        reader().setLineMapper(lineMapper);
        return reader;
    }

    @Bean
    UserProcessor processor() {
        return new UserProcessor();
    }

    @Bean
    FlatFileItemWriter<User> writer() {
        BeanWrapperFieldExtractor<User> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{"firstName", "lastName", "age"});

        DelimitedLineAggregator<User> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(extractor);

        FlatFileItemWriter<User> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output.csv"));
        writer.setShouldDeleteIfExists(true);
        writer.setLineAggregator(aggregator);

        return writer;
    }

    @Bean
    Step dateToAge(
            ItemReader<UserDto> reader,
            ItemProcessor<UserDto, UserDto> processor,
            ItemWriter<User> writer) {

        return stepBuilderFactory.get("dateToAge")
                .<UserDto,User>chunk(100)
                .reader(reader)
                .writer(writer)
                .build();
    }

    @Bean
    Job dateToAgeJob(Step dateToAge){
        return jobBuilderFactory.get("dateToAgeJob")
                .incrementer(new RunIdIncrementer())
                .flow(dateToAge)
                .end().build();
    }

}
