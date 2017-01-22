package com.google.api.service;

import static spark.Spark.port;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.cloud.datastore.Query.ResultType;

import static spark.Spark.get;

public class MainService {

	private int servicePort;
	public MainService(int servicePort) {
		this.servicePort = servicePort;
	}
	
	private final Datastore datastore = DatastoreOptions.defaultInstance().service();
	//private final KeyFactory _keyFactory = datastore.newKeyFactory().kind("HourlySummary");
	
	public void init() {
		System.out.println("port : " + servicePort);
		port(servicePort);
		
		get("/health", (request, response) -> {
			response.status(200);
			return "OK";
		});
		
		get("/" + String.join("/", "v1", "query", "hourly", ":start", ":paras"), (request, response) -> {
			String start = request.params(":start");
			String paras = request.params(":paras");
			String[] words = paras.split("=");
			System.out.println("hourly query start : " + start + " paras : " + paras);
			String key = words[0];
			String value = words[1];
			Query<Entity> query = null;
			if(key.equals("device")) {
				query = Query.newGqlQueryBuilder(
						ResultType.ENTITY, "select * from HourlySummary where device_id = @1 and start >= @2").setAllowLiteral(true).
						addBinding(value).addBinding(start).build();
			} else if(key.equals("event")) {
				query = Query.newGqlQueryBuilder(
						ResultType.ENTITY, "select * from HourlySummary where event = @1 and start >= @2").setAllowLiteral(true).
						addBinding(value).addBinding(start).build();
			}
			QueryResults<Entity> results = datastore.run(query);
			Gson gson = new Gson();
			JsonArray array = new JsonArray();
			try {
				while(results.hasNext()) {
					Entity each = results.next();
					JsonObject eachObj = new JsonObject();
					Double avg = each.getDouble("avg");
					String deviceId = each.getString("device_id");
					String event = each.getString("event");
					Double median = each.getDouble("median");
					long num = each.getLong("number");
					String _start = each.getString("start");
					String _end = each.getString("end");
					eachObj.addProperty("avg", avg);
					eachObj.addProperty("median", median);
					eachObj.addProperty("number", num);
					eachObj.addProperty("device_id", deviceId);
					eachObj.addProperty("event", event);
					eachObj.addProperty("start", _start);
					eachObj.addProperty("end", _end);
					array.add(eachObj);
				}
			}catch(Exception ex) {
				JsonObject obj = new JsonObject();
				obj.addProperty("error", ex.getMessage());
				array.add(obj);
			}
			response.status(200);
			return gson.toJson(array);
		});
		
		get("/" + String.join("/", "v1", "query", "raw", ":start", ":end", ":paras"), (request, response) -> {
			String start = request.params(":start");
			String end = request.params(":end");
			String paras = request.params(":paras");
			String[] words = paras.split("=");
			System.out.println("start : " + start + " end : " + end + " paras : " + paras);
			String key = words[0];
			String value = words[1];
			Query<Entity> query = null;
			if(key.equals("device")) {
				query = Query.newGqlQueryBuilder(
						ResultType.ENTITY, "select * from ParticleEvent where published_at <= @1 and published_at >= @2 and device_id = @3").setAllowLiteral(true).
						addBinding(end).addBinding(start).addBinding(value).build();
			} else if (key.equals("event")) {
				query = Query.newGqlQueryBuilder(
						ResultType.ENTITY, "select * from ParticleEvent where published_at <= @1 and published_at >= @2 and event = @3").setAllowLiteral(true).
						addBinding(end).addBinding(start).addBinding(value).build();
			}
			QueryResults<Entity> results = datastore.run(query);
			Gson gson = new Gson();
			JsonArray array = new JsonArray();
			try {
				while(results.hasNext()) {
					Entity each = results.next();
					JsonObject eachObj = new JsonObject();
					Double dval = null;
					Object dataObj = each.getValue("data");
					if(dataObj instanceof DoubleValue) {
						DoubleValue val = (DoubleValue)dataObj;
						dval = val.get();
					} else if(dataObj instanceof LongValue) {
						LongValue val = (LongValue)dataObj;
						dval = val.get().doubleValue();
					}
					String event = each.getString("event");
					String time = each.getString("published_at");
					String deviceId = each.getString("device_id");
					eachObj.addProperty("data", dval);
					eachObj.addProperty("device_id", deviceId);
					eachObj.addProperty("event", event);
					eachObj.addProperty("published_at", time);					
					array.add(eachObj);
				}
			}catch(Exception ex) {
				JsonObject obj = new JsonObject();
				obj.addProperty("error", ex.getMessage());
				array.add(obj);
			}
			response.status(200);
			return gson.toJson(array);
		});
		
	}
}
