package com.google.api;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.google.api.config.SpringConfig;
import com.google.api.service.RedirectService;

public class ApiAccess {
	private static final Logger LOG = LoggerFactory.getLogger(ApiAccess.class);
	
	public static void main(String[] args) {
		try(AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SpringConfig.class)) {
			LOG.info("start ...");
			RedirectService red = context.getBean(RedirectService.class);
			red.init();
			while(true) {
				try {
					TimeUnit.HOURS.sleep(Integer.MAX_VALUE);
				}catch(InterruptedException e) {}
			}
		}
	}
}
