package com.example.psd_invest_analyzer.statistics;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TestingCollectSink implements SinkFunction<String> {

    public static final Set<String> values = Collections.synchronizedSet(new HashSet<>());

    @Override
    public void invoke(String value, SinkFunction.Context context) {
        values.add(value);
    }
}
