package com.example.psd_invest_analyzer.statistics;

import lombok.RequiredArgsConstructor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor(staticName = "withWindowSize")
class AverageAllWindowFunction implements AllWindowFunction<Double, Double, GlobalWindow> {

    private final int windowSize;

    @Override
    public void apply(GlobalWindow window, Iterable<Double> values, Collector<Double> out) {
        List<Double> samples = StreamSupport.stream(values.spliterator(), false).toList();
        if (samples.size() == windowSize) {
            OptionalDouble average = samples.stream().mapToDouble(sample -> sample).average();
            out.collect(average.isPresent() ? average.getAsDouble() : 0.0);
        }
    }
}
