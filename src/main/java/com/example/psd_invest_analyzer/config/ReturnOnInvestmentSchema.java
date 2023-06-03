package com.example.psd_invest_analyzer.config;

import com.example.psd_investor.ReturnOnInvestmentDto;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

class ReturnOnInvestmentSchema implements DeserializationSchema<ReturnOnInvestmentDto> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ReturnOnInvestmentDto deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, ReturnOnInvestmentDto.class);
    }

    @Override
    public boolean isEndOfStream(ReturnOnInvestmentDto nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ReturnOnInvestmentDto> getProducedType() {
        return TypeExtractor.getForClass(ReturnOnInvestmentDto.class);
    }
}
