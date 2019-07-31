package viettel.DemoHDFS.Tempature.Entity;

import java.sql.Timestamp;
import java.util.UUID;

public class DataObject {
	private UUID id;
	private Timestamp key; // PK
	private String daytime;
	private double avg = 0;
	private double min = 0;
	private double max = 0;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getDaytime() {
		return daytime;
	}

	public void setDaytime(String datetime) {
		this.daytime = datetime;
	}

	public Timestamp getKey() {
		return key;
	}

	public void setKey(Timestamp key) {
		this.key = key;
	}

	public double getAvg() {
		return avg;
	}

	public void setAvg(double avg) {
		this.avg = avg;
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public DataObject() {
		super();
	}

	@Override
	public String toString() {
		return key + ": " + "avg: " + avg + "min: " + min + "max: " + max;
	}
}
