package com.example.psd_invest_analyzer.statistics;

import lombok.RequiredArgsConstructor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor(staticName = "ofOrderWithWindowSize")
class QuantileAllWindowFunction implements AllWindowFunction<Double, Double, GlobalWindow> {

    private final Double order;

    private final int windowSize;

    @Override
    public void apply(GlobalWindow window, Iterable<Double> values, Collector<Double> out) {
        List<Double> samples = StreamSupport.stream(values.spliterator(), false)
                .sorted(Comparator.naturalOrder())
                .toList();
        if (samples.size() == windowSize) {
            int index = (int) Math.ceil(order * samples.size());
            out.collect(samples.get(index - 1));
        }
    }
}
