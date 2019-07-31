package viettel.DemoHDFS.Tempature.Average;

import java.io.Serializable;

public class AverageAgg implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double sum;;
	private long count;

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public AverageAgg(long count, double sum) {
		super();
		this.sum = sum;
		this.count = count;
	}

	public AverageAgg() {
		super();
	}
}
