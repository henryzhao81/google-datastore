package com.google.api.service;

import static spark.Spark.get;
import static spark.Spark.port;

public class RedirectService {

	private int servicePort;
	public RedirectService(int servicePort) {
		this.servicePort = servicePort;
	}
	
	public void init() {
		System.out.println("port : " + servicePort);
		port(servicePort);
		
		get("/health", (request, response) -> {
			response.status(200);
			return "OK";
		});
	}
}
