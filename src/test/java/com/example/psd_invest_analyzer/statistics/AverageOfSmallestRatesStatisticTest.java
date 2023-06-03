package com.example.psd_invest_analyzer.statistics;

import com.example.psd_investor.ReturnOnInvestmentDto;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AverageOfSmallestRatesStatisticTest {

    private StreamExecutionEnvironment environment;

    private DataStream<ReturnOnInvestmentDto> investments;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Before
    public void setUp() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        investments = environment.fromElements(
                ReturnOnInvestmentDto.builder().value(7.0).build(),
                ReturnOnInvestmentDto.builder().value(11.0).build(),
                ReturnOnInvestmentDto.builder().value(19.0).build(),
                ReturnOnInvestmentDto.builder().value(32.0).build(),
                ReturnOnInvestmentDto.builder().value(77.0).build(),
                ReturnOnInvestmentDto.builder().value(79.0).build(),
                ReturnOnInvestmentDto.builder().value(81.0).build(),
                ReturnOnInvestmentDto.builder().value(82.0).build(),
                ReturnOnInvestmentDto.builder().value(83.0).build(),
                ReturnOnInvestmentDto.builder().value(89.0).build(),
                ReturnOnInvestmentDto.builder().value(99.0).build(),
                ReturnOnInvestmentDto.builder().value(999.0).build(),
                ReturnOnInvestmentDto.builder().value(1001.0).build(),
                ReturnOnInvestmentDto.builder().value(1002.0).build()
        );
    }

    @BeforeEach
    void clearCollectingSink() {
        environment.setParallelism(1);
    }

    @Test
    public void determine_ShouldAddSinkToDataStream() throws Exception {
        // Given
        TestingCollectSink.values.clear();
        AverageOfSmallestRatesStatistic statistic = AverageOfSmallestRatesStatistic.builder()
                .windowSize(12)
                .windowSlide(1)
                .percentageOfSmallestStatistics(25)
                .referenceStatistic(999.9)
                .sink(new TestingCollectSink())
                .build();

        // When
        statistic.determine(investments);
        environment.execute();

        // Then
        DoubleToStringWithTimestampMapper mapper = new DoubleToStringWithTimestampMapper();
        assertEquals(3, TestingCollectSink.values.size());
        assertEquals(Set.of(mapper.map(37.0/3.0),
                        mapper.map(62.0/3.0),
                        mapper.map(128.0/3.0)),
                TestingCollectSink.values);
    }


}
