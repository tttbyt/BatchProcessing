package viettel.DemoHDFS.HeartRate.Entity;

import java.sql.Timestamp;
import java.util.UUID;

public class HeartRateAvg {
	private UUID id;
	private Timestamp day; 
	private String daytime;
	private double heart_rate_avg = 0;
	private double heart_rate_min = 0;
	private double heart_rate_max = 0;
	public UUID getId() {
		return id;
	}
	public void setId(UUID id) {
		this.id = id;
	}
	public Timestamp getDay() {
		return day;
	}
	public void setDay(Timestamp day) {
		this.day = day;
	}
	public String getDaytime() {
		return daytime;
	}
	public void setDaytime(String daytime) {
		this.daytime = daytime;
	}
	public double getHeart_rate_avg() {
		return heart_rate_avg;
	}
	public void setHeart_rate_avg(double heart_rate_avg) {
		this.heart_rate_avg = heart_rate_avg;
	}
	public double getHeart_rate_min() {
		return heart_rate_min;
	}
	public void setHeart_rate_min(double heart_rate_min) {
		this.heart_rate_min = heart_rate_min;
	}
	public double getHeart_rate_max() {
		return heart_rate_max;
	}
	public void setHeart_rate_max(double heart_rate_max) {
		this.heart_rate_max = heart_rate_max;
	}
	public HeartRateAvg() {
	}
	
}
