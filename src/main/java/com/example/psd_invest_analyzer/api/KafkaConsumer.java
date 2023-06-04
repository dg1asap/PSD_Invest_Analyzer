package com.example.psd_invest_analyzer.api;

import com.example.psd_investor.ReturnOnInvestmentDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "Investment_A", groupId = "group_id")
    public void consumeInvestmentsFromA(ReturnOnInvestmentDto dto) {
        String consumerMessage = String.format("Investment_A: %f", dto.getValue());
        logger.info(consumerMessage);
    }

    @KafkaListener(topics = "Investment_B", groupId = "group_id")
    public void consumeInvestmentsFromB(ReturnOnInvestmentDto dto) {
        String consumerMessage = String.format("Investment_B: %f", dto.getValue());
        logger.info(consumerMessage);
    }

    @KafkaListener(topics = "Investment_C", groupId = "group_id")
    public void consumeInvestmentsFromC(ReturnOnInvestmentDto dto) {
        String consumerMessage = String.format("Investment_C: %f", dto.getValue());
        logger.info(consumerMessage);
    }

    @KafkaListener(topics = "Investment_D", groupId = "group_id")
    public void consumeInvestmentsFormD(ReturnOnInvestmentDto dto) {
        String consumerMessage = String.format("Investment_D: %f", dto.getValue());
        logger.info(consumerMessage);
    }

    @KafkaListener(topics = "Investment_E", groupId = "group_id")
    public void consumeInvestmentsFormE(ReturnOnInvestmentDto dto) {
        String consumerMessage = String.format("Investment_E: %f", dto.getValue());
        logger.info(consumerMessage);
    }
}
