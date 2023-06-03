package com.example.psd_invest_analyzer.api;

import com.example.psd_invest_analyzer.statistics.InvestmentStatistic;
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


@Component
public class StatisticCalculatorController implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(StatisticCalculatorController.class);

    private final ImmutablePair<DataStream<ReturnOnInvestmentDto>, StreamExecutionEnvironment> flinkKafkaDataStream;

    private final List<InvestmentStatistic> investmentStatistics;

    public StatisticCalculatorController(@Qualifier("flinkKafkaDataStream") ImmutablePair<DataStream<ReturnOnInvestmentDto>, StreamExecutionEnvironment> flinkKafkaDataStream,
                                         @Qualifier("investmentsStatistics") List<InvestmentStatistic> investmentStatistics) {
        this.flinkKafkaDataStream = flinkKafkaDataStream;
        this.investmentStatistics = investmentStatistics;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        try {
            DataStream<ReturnOnInvestmentDto> investments = flinkKafkaDataStream.getLeft();

            investmentStatistics.forEach(statistic -> statistic.determine(investments));

            StreamExecutionEnvironment environment = flinkKafkaDataStream.getRight();
            environment.execute("Analyze investment");
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }




}
