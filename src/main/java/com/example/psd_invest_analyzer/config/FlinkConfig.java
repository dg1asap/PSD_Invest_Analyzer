package com.example.psd_invest_analyzer.config;

import com.example.psd_investor.ReturnOnInvestmentDto;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
public class FlinkConfig {

    @Bean(name = "flinkKafkaDataStream")
    public ImmutablePair<Map<String, DataStream<ReturnOnInvestmentDto>>, StreamExecutionEnvironment> flinkKafkaDataStream() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(50000);

        List<String> topicNames = getInvestmentsTopicName();
        Map<String, DataStream<ReturnOnInvestmentDto>> dataStreams = new HashMap<>();
        Properties properties = getProperties();
        topicNames.forEach(topicName -> dataStreams.put(topicName,
                getReturnOnInvestmentDtoDataStreamForTopic(properties, environment, topicName)));

        return new ImmutablePair<>(dataStreams, environment);
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "myConsumerGroup");
        return properties;
    }

    @Bean
    StreamExecutionEnvironment streamExecutionEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static List<String> getInvestmentsTopicName() {
        return List.of("Investment_A", "Investment_B", "Investment_C", "Investment_D", "Investment_E");
    }

    private static DataStream<ReturnOnInvestmentDto> getReturnOnInvestmentDtoDataStreamForTopic(Properties properties, StreamExecutionEnvironment environment, String topicName) {
        return environment.addSource(new FlinkKafkaConsumer<>(topicName, new ReturnOnInvestmentSchema(), properties));
    }
}
