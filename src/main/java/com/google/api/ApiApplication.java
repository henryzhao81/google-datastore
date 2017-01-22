package com.google.api;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.google.api.config.SpringConfig;
import com.google.api.service.MainService;

public class ApiApplication {
	private static final Logger LOG = LoggerFactory.getLogger(ApiApplication.class);
	
	public static void main(String[] args) {
		try(AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SpringConfig.class)) {
			LOG.info("start ...");
			MainService api = context.getBean(MainService.class);
			api.init();
			while(true) {
				try {
					TimeUnit.HOURS.sleep(Integer.MAX_VALUE);
				}catch(InterruptedException e) {}
			}
		}
	}
}
