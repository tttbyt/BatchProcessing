package viettel.DemoHDFS;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MyAverage extends UserDefinedAggregateFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private StructType inputSchema;// input
	private StructType bufferSchema;// save value during calculator

	public MyAverage() {
		List<StructField> input = new ArrayList<StructField>();
		input.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
		inputSchema = DataTypes.createStructType(input);
		// inputSchema read Longtype

		List<StructField> buf = new ArrayList<StructField>();
		buf.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
		buf.add(DataTypes.createStructField("count", DataTypes.LongType, true));
		bufferSchema = DataTypes.createStructType(buf);
	}

	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public DataType dataType() {
		return DataTypes.DoubleType;
		// data return of calculation
	}

	@Override
	public boolean deterministic() {
		return true;
	}

	@Override
	public Object evaluate(Row buffer) {
		return ((double) buffer.getLong(0)) / buffer.getLong(1);
		// call when perform calculation
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		// update bufferSchema
		buffer.update(0, 0L);
		buffer.update(1, 0L);
	}

	@Override
	public StructType inputSchema() {
		return inputSchema;
	}

	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		// merge 2 buffer
		buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0));
		buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
	}

	@Override
	public void update(MutableAggregationBuffer buffer1, Row input) {
		// update buffer with Row value
		if (input.isNullAt(0)) {
			//if value at coll 0 of row not null
			buffer1.update(0, buffer1.getLong(0) + input.getLong(0));
			buffer1.update(1, buffer1.getLong(1) + 1);
		}
	}

}
