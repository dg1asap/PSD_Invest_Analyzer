package com.example.psd_invest_analyzer.statistics;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class StatisticCalculatorCreator {

    private static final String STATISTIC_MAIN_DIRECTORY = "./statistic/";

    public List<InvestmentStatistic> createInvestmentStatisticsWithName(String investmentName) {
        AverageStatistic averageStatistic = createAverageStatisticWithName(investmentName);
        QuantileStatistic quantileStatistic = createQuantileStatisticWithName(investmentName);
        AverageOfSmallestRatesStatistic averageOfSmallestRatesStatistic =
                createAverageOfSmallRatesStatisticWithName(investmentName);
        return List.of(averageStatistic, quantileStatistic, averageOfSmallestRatesStatistic);
    }

    private AverageOfSmallestRatesStatistic createAverageOfSmallRatesStatisticWithName(String investmentName) {
        return AverageOfSmallestRatesStatistic.builder()
                .windowSize(30)
                .windowSlide(1)
                .percentageOfSmallestStatistics(10)
                .referenceStatistic(0.07)
                .exceeding(0.01)
                .sink(createStreamingFileSinkWithPath(
                        STATISTIC_MAIN_DIRECTORY + investmentName + "/average_smallest_rates"))
                .build();
    }

    private QuantileStatistic createQuantileStatisticWithName(String investmentName) {
        return QuantileStatistic.builder()
                .windowSize(30)
                .windowSlide(1)
                .quantileOrder(0.1)
                .referenceStatistic(-0.0808)
                .exceeding(0.01)
                .sink(createStreamingFileSinkWithPath(
                        STATISTIC_MAIN_DIRECTORY + investmentName + "/quantile"))
                .build();
    }

    private AverageStatistic createAverageStatisticWithName(String investmentName) {
        return AverageStatistic.builder()
                .windowSize(30)
                .windowSlide(1)
                .referenceStatistic(0.0101)
                .exceeding(0.01)
                .sink(createStreamingFileSinkWithPath(
                        STATISTIC_MAIN_DIRECTORY + investmentName + "/average"))
                .build();
    }

    private StreamingFileSink<String> createStreamingFileSinkWithPath(String path) {
        return StreamingFileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 10)
                        .build())
                .build();
    }
}
