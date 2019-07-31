package viettel.DemoHDFS.Tempature.Entity;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;

public class TempatureMoment implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private UUID id;
	private Timestamp upTime;
	private double value;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public Timestamp getUpTime() {
		return upTime;
	}

	public void setUpTime(Timestamp upTime) {
		this.upTime = upTime;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public TempatureMoment() {
	}

	public TempatureMoment(UUID id, Timestamp upTime, double value) {
		super();
		this.id = id;
		this.upTime = upTime;
		this.value = value;
	}
}
