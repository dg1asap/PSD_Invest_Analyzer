package com.example.psd_invest_analyzer.statistics;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class StatisticCalculatorConfig {

    @Bean(name = "investmentsStatistics")
    public List<InvestmentStatistic> investmentStatistics() {
        AverageStatistic averageStatistic =
                AverageStatistic.builder()
                        .windowSize(30)
                        .windowSlide(1)
                        .referenceStatistic(0.0)
                        .sink(createStreamingFileSinkWithPath("./statistic/average"))
                        .build();

        QuantileStatistic quantileStatistic =
                QuantileStatistic.builder()
                        .windowSize(30)
                        .windowSlide(1)
                        .quantileOrder(0.1)
                        .referenceStatistic(0.0)
                        .sink(createStreamingFileSinkWithPath("./statistic/quantile"))
                        .build();

        AverageOfSmallestRatesStatistic averageOfSmallestRatesStatistic =
                AverageOfSmallestRatesStatistic.builder()
                        .windowSize(30)
                        .windowSlide(1)
                        .percentageOfSmallestStatistics(10)
                        .referenceStatistic(0.0)
                        .sink(createStreamingFileSinkWithPath("./statistic/average_smallest_rates"))
                        .build();

        return List.of(averageStatistic, quantileStatistic, averageOfSmallestRatesStatistic);
    }

    private StreamingFileSink<String> createStreamingFileSinkWithPath(String path) {
        return StreamingFileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketCheckInterval(1000)
                .build();
    }
}
