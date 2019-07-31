package viettel.DemoHDFS.BloodPressure.Entity;

import java.sql.Timestamp;
import java.util.UUID;

public class BloodPressureAvg {
	private UUID id;
	private Timestamp day;
	private String daytime;
	private double high_avg = 0;
	private double low_avg = 0;

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

	public double getHigh_avg() {
		return high_avg;
	}

	public void setHigh_avg(double high_avg) {
		this.high_avg = high_avg;
	}

	public double getLow_avg() {
		return low_avg;
	}

	public void setLow_avg(double low_avg) {
		this.low_avg = low_avg;
	}

	public BloodPressureAvg() {
		
	}
}
