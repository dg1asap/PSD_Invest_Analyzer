package com.example.psd_invest_analyzer.api;

import com.example.psd_investor.ReturnOnInvestmentDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "NewTopic", groupId = "group_id")
    public void consumer(ReturnOnInvestmentDto dto) {
        String consumerMessage = String.format("C: %f", dto.getValue());
        logger.info(consumerMessage);
    }
}
