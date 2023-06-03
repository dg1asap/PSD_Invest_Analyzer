package com.example.psd_invest_analyzer.statistics;

import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

class DoubleToStringWithTimestampMapper implements MapFunction<Double, String> {

    @Override
    public String map(Double value) {
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        String timestamp = dateFormat.format(date);
        return value.toString() + ";" + timestamp;
    }
}
