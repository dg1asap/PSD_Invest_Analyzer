package com.example.psd_invest_analyzer.statistics;

import lombok.RequiredArgsConstructor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor(staticName = "fromNSamplesAndWindowSize")
class SumOfSmallestRatesOfReturn implements AllWindowFunction<Double, Double, GlobalWindow> {

    private final Integer numberOfSamples;

    private final int windowSize;

    @Override
    public void apply(GlobalWindow window, Iterable<Double> values, Collector<Double> out) {
        List<Double> samples = StreamSupport.stream(values.spliterator(), false).toList();
        if (samples.size() == windowSize) {
            Double average = samples.stream()
                    .sorted(Comparator.naturalOrder())
                    .limit(numberOfSamples)
                    .reduce(0.0, Double::sum);
            out.collect(average/ numberOfSamples);
        }
    }
}
