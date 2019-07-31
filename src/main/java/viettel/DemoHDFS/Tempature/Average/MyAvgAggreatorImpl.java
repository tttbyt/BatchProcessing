package viettel.DemoHDFS.Tempature.Average;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;

public class MyAvgAggreatorImpl extends Aggregator<Row, AverageAgg, Double> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Encoder<AverageAgg> bufferEncoder() {
		return Encoders.bean(AverageAgg.class);
	}

	@Override
	public Double finish(AverageAgg reduction) {
		return reduction.getSum() / reduction.getCount();
	}

	@Override
	public AverageAgg merge(AverageAgg b1, AverageAgg b2) {
		b1.setCount(b1.getCount() + b2.getCount());
		b1.setSum(b1.getSum() + b2.getSum());
		return b1;
	}

	@Override
	public Encoder<Double> outputEncoder() {
		return Encoders.DOUBLE();
	}

	@Override
	public AverageAgg reduce(AverageAgg b, Row a) {
		if (a != null) {
			b.setCount(b.getCount() + 1);
			b.setSum(b.getSum() + a.getDouble(2));
		}
		return b;
	}

	
	public AverageAgg zero() {
		return new AverageAgg(0L, 0.0);
	}

}
