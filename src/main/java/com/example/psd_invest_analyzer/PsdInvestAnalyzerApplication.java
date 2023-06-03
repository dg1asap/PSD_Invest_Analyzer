package com.example.psd_invest_analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.example.psd_invest_analyzer.api"})
@ComponentScan(basePackages = {"com.example.psd_invest_analyzer.config"})
@ComponentScan(basePackages = {"com.example.psd_invest_analyzer.statistics"})
@ComponentScan(basePackages = {"com.example.psd_investor"})
public class PsdInvestAnalyzerApplication {

	public static void main(String[] args) {
		SpringApplication.run(PsdInvestAnalyzerApplication.class, args);
	}
}
