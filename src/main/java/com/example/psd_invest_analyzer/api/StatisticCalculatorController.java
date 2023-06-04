package com.example.psd_invest_analyzer.api;

import com.example.psd_invest_analyzer.statistics.InvestmentStatistic;
import com.example.psd_invest_analyzer.statistics.StatisticCalculatorCreator;
import com.example.psd_investor.ReturnOnInvestmentDto;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;


@Component
public class StatisticCalculatorController implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(StatisticCalculatorController.class);

    private final ImmutablePair<Map<String, DataStream<ReturnOnInvestmentDto>>, StreamExecutionEnvironment> flinkKafkaDataStream;

    private final StatisticCalculatorCreator statisticCalculatorCreator;

    public StatisticCalculatorController(@Qualifier("flinkKafkaDataStream") ImmutablePair<Map<String, DataStream<ReturnOnInvestmentDto>>, StreamExecutionEnvironment> flinkKafkaDataStream,
                                         StatisticCalculatorCreator statisticCalculatorCreator) {
        this.flinkKafkaDataStream = flinkKafkaDataStream;
        this.statisticCalculatorCreator = statisticCalculatorCreator;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        try {
            Map<String, DataStream<ReturnOnInvestmentDto>> investments = flinkKafkaDataStream.getLeft();
            investments.forEach(this::assignStatisticsToInvestment);

            StreamExecutionEnvironment environment = flinkKafkaDataStream.getRight();
            environment.execute("Analyze investment");
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private void assignStatisticsToInvestment(String investmentName, DataStream<ReturnOnInvestmentDto> investment) {
        List<InvestmentStatistic> statistics = statisticCalculatorCreator.createInvestmentStatisticsWithName(investmentName);
        statistics.forEach(statistic -> statistic.determine(investment));
    }


}
