package viettel.DemoHDFS.BloodPressure.Entity;

import java.sql.Timestamp;
import java.util.UUID;

public class BloodPressureCurrent {
	private UUID id;
	private Timestamp time;
	private double high_value;
	private double low_value;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public Timestamp getTime() {
		return time;
	}

	public void setTime(Timestamp time) {
		this.time = time;
	}

	public double getHigh_value() {
		return high_value;
	}

	public void setHigh_value(double high_value) {
		this.high_value = high_value;
	}

	public double getLow_value() {
		return low_value;
	}

	public void setLow_value(double low_value) {
		this.low_value = low_value;
	}

	public BloodPressureCurrent() {

	}
}
