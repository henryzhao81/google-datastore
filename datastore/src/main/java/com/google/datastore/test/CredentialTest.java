package com.google.datastore.test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.DateTime;
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.Query.ResultType;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.common.util.concurrent.AtomicDouble;

public class CredentialTest {

	// Create an authorized Datastore service using Application Default Credentials.
	private final Datastore datastore = DatastoreOptions.defaultInstance().service();
	// Create a Key factory to construct keys associated with this project.
	private final KeyFactory keyFactory = datastore.newKeyFactory().kind("MetaData");
	
	public static void main(String[] args) throws Exception {
		CredentialTest test = new CredentialTest();
		/*
		Iterator<Entity> iter = test.listResults();
		while(iter.hasNext()) {
			Entity each = iter.next();
			System.out.println("data : " + each.getValue("data").toString() 
			+ " device_id : " + each.getString("device_id") 
			+ " event : " + each.getString("event") 
			+ " published_at : " + each.getString("published_at"));
		}*/
		String start = "2017-01-20T22:00:00.000Z";
		String end =   "2017-01-22T00:00:00.000Z";
		while(true) {
			String next = test.getNextHour(start);
			System.out.println("start : " + start + " next : " + next);
			if(next.equals(end)) {
				System.out.println("stop");
				return;
			}
			test.calSummary(start, next);
			start = next;
		}
		//String end = "2017-02-10T00:00:00.000Z";
		//test.getSummary(start, end);
	}
	
	String getNextHour(String start) throws ParseException {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	    Date date = formatter.parse(start);
	    Calendar calendar = Calendar.getInstance();
	    calendar.setTime(date);
	    calendar.add(Calendar.HOUR, 1);
	    return formatter.format(calendar.getTime());
	}
	
	boolean isSmaller(String start, String end) throws ParseException {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	    Date date1 = formatter.parse(start);
	    Date date2 = formatter.parse(end);
	    if(date1.getTime() < date2.getTime())
	    	return true;
	    return false;
	}
	
	/**
	 * Adds a task entity to the Datastore.
	 *
	 * @param description The task description
	 * @return The {@link Key} of the entity
	 * @throws DatastoreException if the ID allocation or put fails
	 */
	Key addTask(String description) {
	  Key key = datastore.allocateId(keyFactory.newKey());
	  Entity task = Entity.builder(key)
	      .set("description", StringValue.builder(description).excludeFromIndexes(true).build())
	      .set("created", DateTime.now())
	      .set("done", false)
	      .build();
	  datastore.put(task);
	  return key;
	}
	
	/**
	 * Returns a list of all task entities in ascending order of creation time.
	 *
	 * @throws DatastoreException if the query fails
	 */
	Iterator<Entity> listTasks() {
	  Query<Entity> query = Query.entityQueryBuilder().kind("ParticleEvent").orderBy(OrderBy.asc("published_at")).build();
	  return datastore.run(query);
	}
	
	Iterator<Entity> listResults() {
//		Query<Entity> query =
//		        Query.newGqlQueryBuilder(
//		            ResultType.ENTITY, "select * from ParticleEvent where published_at < @1 and published_at > @2 and device_id = @3").setAllowLiteral(true).
//		        addBinding("2016-10-23T22:30:00.000Z").addBinding("2016-10-23T22:00:00.000Z").addBinding("330021000d47343432313031")
//		        .build();
		Query<Entity> query =
		        Query.newGqlQueryBuilder(
		            ResultType.ENTITY, "select * from ParticleEvent where published_at < @1 and published_at > @2").setAllowLiteral(true).
		        addBinding("2016-10-23T22:30:00.000Z").addBinding("2016-10-23T22:00:00.000Z")
		        .build();
		QueryResults<Entity> results = datastore.run(query);
        return results;
	}
	
	void testInsert() {
		FullEntity task = Entity.newBuilder(keyFactory.newKey()).
				set("city", "san francisco").set("compost", "64").set("country", "US").set("cuisine", "Thai")
				.set("device_id", "330021000d47343432313031").set("landfill", "32").set("pickups", "2")
				.set("recycle", "64").set("restaurant_id", "0000000001").set("restaurant_name", "little thai").set("state", "CA").build();
		datastore.add(task);
	}
	
	private final static String TYPE_LANDFILL = "landfill";
	private final static String TYPE_COMPOST = "Compost";
	private final static String TYPE_RECYCLE = "Recycle";
	
    void calSummary(String start, String end) {    	
    	Map<String, List<Pair>> map = this.getSummary(start, end);
    	if(map != null && map.size() > 0) {
    		Iterator<Map.Entry<String, List<Pair>>> keys = map.entrySet().iterator();
    		while(keys.hasNext()) {
    			Map.Entry<String, List<Pair>> each = keys.next();
    			String strKey = each.getKey();
    			System.out.println("initial device id : " + strKey);
    			//------Initial count, total and list per each device id------//
    	    	AtomicDouble total_landfill = new AtomicDouble();
    	    	AtomicDouble total_compost = new AtomicDouble();
    	    	AtomicDouble total_recycle = new AtomicDouble();
    	    	
    	    	AtomicInteger count_landfill = new AtomicInteger();
    	    	AtomicInteger count_compost = new AtomicInteger();
    	    	AtomicInteger count_recycle = new AtomicInteger();
    	    	
    	    	List<Double> list_landfill = new ArrayList<Double>();
    	    	List<Double> list_compost = new ArrayList<Double>();
    	    	List<Double> list_recycle = new ArrayList<Double>();
    	    	//------------------------------------------------------------//
    			List<Pair> vals = each.getValue();
    			System.out.println("pair size for device id : " + strKey + " is " + vals.size());
    			for(Pair val : vals) {
    				if(val.event.equals(TYPE_LANDFILL)) {
    					total_landfill.getAndAdd(val.value);
    					count_landfill.getAndIncrement();
    					list_landfill.add(val.value);
    				} else if(val.event.equals(TYPE_COMPOST)) {
    					total_compost.getAndAdd(val.value);
    					count_compost.getAndIncrement();
    					list_compost.add(val.value);
    				} else if(val.event.equals(TYPE_RECYCLE)) {
    					total_recycle.getAndAdd(val.value);
    					count_recycle.getAndIncrement();
    					list_recycle.add(val.value);
    				} else {
    					System.out.println("Unrecognized type : " + val.event);
    				}
    			}
    			int c_l = count_landfill.get();
    			int c_c = count_compost.get();
    			int c_r = count_recycle.get();
    			if(c_l > 0) {
    				double res = total_landfill.get() / c_l;
    				Collections.sort(list_landfill);
    				double median = list_landfill.get(c_l / 2);
    				System.out.println("device id : " + strKey + " event : landfill " + " size : " + c_l + " avg : " + res + " start : " + start + " end : " + end + " median : " + median);
    				//this.insertSummary(strKey, res, TYPE_LANDFILL, start, end, c_l, median);
    			}
    			if(c_c > 0) {
    				double res = total_compost.get() / c_c;
    				Collections.sort(list_compost);
    				double median = list_compost.get(c_c / 2);
    				System.out.println("device id : " + strKey + " event : compost " + " size : " + c_c + " avg : " + res + " start : " + start + " end : " + end + " median : " + median);
    				//this.insertSummary(strKey, res, TYPE_COMPOST, start, end, c_c, median);
    			}
    			if(c_r > 0) {
    				double res = total_recycle.get() / c_r;
    				Collections.sort(list_recycle);
    				double median = list_recycle.get(c_r / 2);
    				System.out.println("device id : " + strKey + " event : recycle " + " size : " + c_r + " avg : " + res + " start : " + start + " end : " + end + " median : " + median);
    				//this.insertSummary(strKey, res, TYPE_RECYCLE, start, end, c_r, median);
    			}
    		}
    	}
    }
    
    private final KeyFactory _keyFactory = datastore.newKeyFactory().kind("HourlySummary");
    void insertSummary(String deviceId, double avgVal, String event, String start, String end, int num, double median) {
    	FullEntity task = Entity.newBuilder(_keyFactory.newKey()).set("device_id", deviceId).
    			set("avg", avgVal).set("event", event).set("start", start).set("end", end).set("median", median).set("number", num).build();
    	datastore.add(task);
    }
    
    static class Pair {
    	String event;
    	Double value;
    	
    	public Pair(String event, Double value) {
    		this.event = event;
    		this.value = value;
    	}
    }
	
	Map<String, List<Pair>> getSummary(String start, String end) {
		System.out.println("start : " + start + " end : " + end);
		Query<Entity> query =
		        Query.newGqlQueryBuilder(
		            ResultType.ENTITY, "select * from ParticleEvent where published_at < @1 and published_at > @2").setAllowLiteral(true).
		        addBinding(end).addBinding(start).build();
		QueryResults<Entity> results = datastore.run(query);
		Map<String, List<Pair>> map = new HashMap<String, List<Pair>>();
		while(results.hasNext()) {
			Entity each = results.next();
			String deviceId = each.getString("device_id");
			Double dval = null;
			Object dataObj = each.getValue("data");
			if(dataObj instanceof DoubleValue) {
				DoubleValue val = (DoubleValue)dataObj;
				dval = val.get();
			} else if(dataObj instanceof LongValue) {
				LongValue val = (LongValue)dataObj;
				dval = val.get().doubleValue();
			} else {
				System.out.println("unrecognized data type : " + dataObj.toString());
			}
			String eventType = each.getString("event");
			Pair p = null;
			if(dval != null) {
				p = new Pair(eventType, dval);
			}
			if(!map.containsKey(deviceId)) {
				List<Pair> list = new ArrayList<Pair>();
				if(p != null)
				    list.add(p);
				map.put(deviceId, list);
			} else {
				List<Pair> list = map.get(deviceId);
				if(p != null)
				    list.add(p);
			}		
		}
		return map;		
	}

}
