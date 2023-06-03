package com.example.psd_invest_analyzer.statistics;

import com.example.psd_investor.ReturnOnInvestmentDto;
import lombok.Builder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

@Builder
class QuantileStatistic implements InvestmentStatistic {


    private final int windowSize;

    private final int windowSlide;

    private final double quantileOrder;

    private final double referenceStatistic;

    private final SinkFunction<String> sink;

    @Override
    public void determine(DataStream<ReturnOnInvestmentDto> investments) {
        investments.map(ReturnOnInvestmentDto::getValue)
                .countWindowAll(windowSize ,windowSlide)
                .apply(QuantileAllWindowFunction.ofOrderWithWindowSize(quantileOrder, windowSize))
                .filter(FilterOfExceedingValueOfReferencesStatistics
                        .withRefAndExceeding(referenceStatistic, 0.01))
                .map(new DoubleToStringWithTimestampMapper())
                .addSink(sink);
    }
}
