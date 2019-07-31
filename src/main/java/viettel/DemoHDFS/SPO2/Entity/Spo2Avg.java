package viettel.DemoHDFS.SPO2.Entity;

import java.sql.Timestamp;
import java.util.UUID;

public class Spo2Avg {
	private UUID id;
	private Timestamp day;
	private String daytime;
	private double spo2_avg = 0;
	private double spo2_min = 0;
	private double spo2_max = 0;

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

	public double getSpo2_avg() {
		return spo2_avg;
	}

	public void setSpo2_avg(double spo2_avg) {
		this.spo2_avg = spo2_avg;
	}

	public double getSpo2_min() {
		return spo2_min;
	}

	public void setSpo2_min(double spo2_min) {
		this.spo2_min = spo2_min;
	}

	public double getSpo2_max() {
		return spo2_max;
	}

	public void setSpo2_max(double spo2_max) {
		this.spo2_max = spo2_max;
	}

	public Spo2Avg() {

	}

}
