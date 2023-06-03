package com.example.psd_invest_analyzer.statistics;

import com.example.psd_investor.ReturnOnInvestmentDto;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface InvestmentStatistic {

    void determine(DataStream<ReturnOnInvestmentDto> investments);
}
