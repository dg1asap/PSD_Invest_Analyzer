package com.example.psd_invest_analyzer.statistics;

import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;

@RequiredArgsConstructor(staticName = "withRefAndExceeding")
class FilterOfExceedingValueOfReferencesStatistics implements FilterFunction<Double> {

    private final Double referenceStatistic;

    private final Double exceeding;

    @Override
    public boolean filter(Double statistic) {
        return ((referenceStatistic - statistic) / (1 + referenceStatistic)) >= exceeding;
    }
}
