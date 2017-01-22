package com.google.api.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.google.api.service.MainService;

@Configuration
@PropertySource(value = {"file:${api.properties}"})
public class SpringConfig {
	@Value("${api.server.port}")
	private int apiServerPort;
	
	@Bean
	public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
		return new PropertySourcesPlaceholderConfigurer(); 
	}
	
	@Bean
	public MainService apiApplication() throws Exception {
		return new MainService(apiServerPort);
	}
}
