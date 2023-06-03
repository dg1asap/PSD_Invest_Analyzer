package com.example.psd_invest_analyzer.config;

import com.example.psd_investor.ReturnOnInvestmentDto;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class FlinkConfig {

    @Bean(name = "flinkKafkaDataStream")
    public ImmutablePair<DataStream<ReturnOnInvestmentDto>, StreamExecutionEnvironment> flinkKafkaDataStream() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "myConsumerGroup");

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<ReturnOnInvestmentDto> dataStream = environment.addSource(new FlinkKafkaConsumer<>(
                "NewTopic", new ReturnOnInvestmentSchema(), properties));
        return new ImmutablePair<>(dataStream, environment);
    }

    @Bean
    StreamExecutionEnvironment streamExecutionEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
