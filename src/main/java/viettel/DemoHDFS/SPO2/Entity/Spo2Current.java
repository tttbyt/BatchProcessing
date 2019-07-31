package viettel.DemoHDFS.SPO2.Entity;

import java.sql.Timestamp;
import java.util.UUID;

public class Spo2Current {
	private UUID id;
	private Timestamp time;
	private double spo2_current;

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

	public double getSpo2_current() {
		return spo2_current;
	}

	public void setSpo2_current(double spo2_current) {
		this.spo2_current = spo2_current;
	}

	public Spo2Current() {

	}
}
