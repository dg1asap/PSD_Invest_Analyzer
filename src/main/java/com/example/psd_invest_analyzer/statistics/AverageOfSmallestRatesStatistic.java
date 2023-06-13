package com.example.psd_invest_analyzer.statistics;

import com.example.psd_investor.ReturnOnInvestmentDto;
import lombok.Builder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;

@Builder
class AverageOfSmallestRatesStatistic implements InvestmentStatistic, Serializable {

    private final int windowSize;

    private final int windowSlide;

    private final int percentageOfSmallestStatistics;

    private final double referenceStatistic;

    private final double exceeding;

    private final SinkFunction<String> sink;

    @Override
    public void determine(DataStream<ReturnOnInvestmentDto> investments) {
        investments.map(ReturnOnInvestmentDto::getValue)
                .countWindowAll(windowSize,windowSlide)
                .apply(AverageOfSmallestRatesOfReturn.fromNSamplesAndWindowSize(
                        percentageOfSmallestStatistics*windowSize/100, windowSize))
                .filter(FilterOfExceedingValueOfReferencesStatistics
                        .withRefAndExceeding(referenceStatistic, exceeding))
                .map(new DoubleToStringWithTimestampMapper())
                .addSink(sink);
    }
}
