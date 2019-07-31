package viettel.DemoHDFS;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;

import viettel.DemoHDFS.BloodPressure.Entity.BloodPressureAvg;
import viettel.DemoHDFS.BloodPressure.Entity.BloodPressureCurrent;
import viettel.DemoHDFS.HeartRate.Entity.HeartRateAvg;
import viettel.DemoHDFS.HeartRate.Entity.HeartRateCurrent;
import viettel.DemoHDFS.SPO2.Entity.Spo2Avg;
import viettel.DemoHDFS.SPO2.Entity.Spo2Current;
import viettel.DemoHDFS.Tempature.Average.MyAvgAggreatorImpl;
import viettel.DemoHDFS.Tempature.Entity.DataObject;
import viettel.DemoHDFS.Tempature.Entity.TempatureMoment;

public class App {
	final static Logger logger = Logger.getLogger(App.class);
	static CassandraConnector client = CassandraConnector.getInstance();

	/*
	 * arg[0]: tempature arg[1]: heart_rate arg[2]: spo2 arg[3]: blood_pressure
	 */

	public static void main(String[] args) {

		System.out.println("START");
		String nameserviceId = "mycluster";
		{
			try {
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS", "hdfs://mycluster");
				conf.set("dfs.client.failover.proxy.provider." + nameserviceId,
						"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
				conf.set("dfs.nameservices", nameserviceId);
				conf.set("dfs.ha.namenodes." + nameserviceId, "nn1,nn2");
				conf.set("dfs.namenode.rpc-address." + nameserviceId + ".nn1", "10.55.123.52:9000");
				conf.set("dfs.namenode.rpc-address." + nameserviceId + ".nn2", "10.55.123.60:9000");
				conf.set("dfs.namenode.http-address." + nameserviceId + ".nn1", "10.55.123.52:9870");
				conf.set("dfs.namenode.http-address." + nameserviceId + ".nn2", "10.55.123.60:9870");
				conf.set("dfs.replication", "2");

				FileSystem fileSystem = FileSystem.get(conf);
				Path destPath = new Path(args[0]);
				if (!fileSystem.exists(destPath)) {
					logger.info("File doesnot exist! Exit program!!!!!");
					System.exit(1);
				}

				// write to file => exist

			} catch (Exception e) {
				e.printStackTrace();
				logger.info("Program error: " + e);
				System.exit(1);
			}
		}
		// action
		if (args.length > 0) {
			try {
				Calendar startTime = Calendar.getInstance();
				logger.info("Program starting: " + startTime.getTimeInMillis());
				SparkConf conf = new SparkConf();
				conf.setAppName("DemoHDFSConf");

				conf.set("dfs.client.failover.proxy.provider." + nameserviceId,
						"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
				conf.set("dfs.nameservices", nameserviceId);
				conf.set("dfs.ha.namenodes." + nameserviceId, "nn1,nn2");
				conf.set("dfs.namenode.rpc-address." + nameserviceId + ".nn1", "10.55.123.52:9000");
				conf.set("dfs.namenode.rpc-address." + nameserviceId + ".nn2", "10.55.123.60:9000");
				conf.set("dfs.namenode.http-address." + nameserviceId + ".nn1", "10.55.123.52:9870");
				conf.set("dfs.namenode.http-address." + nameserviceId + ".nn2", "10.55.123.52:9870");

				SQLContext sqlContext = SparkSession.builder().config(conf).getOrCreate().sqlContext();
				/* temparute */
				Dataset<TempatureMoment> df = sqlContext.read().json("hdfs://" + nameserviceId + args[0])
						.as(Encoders.bean(TempatureMoment.class));
				df.printSchema();

				Calendar cal = Calendar.getInstance();
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				//cal.add(Calendar.DATE, -2);
				sqlContext.udf().register("calAvgDay", new MyAverage());// register an userdefine function at context,
																		// using with SQL cmd

				MyAvgAggreatorImpl newAvg = new MyAvgAggreatorImpl();
				TypedColumn<Row, Double> typedCol = newAvg.toColumn().name("avgbyday");

				Dataset<Row> datasetByDay = df.where(df.col("upTime").gt(cal.getTimeInMillis())).groupBy(df.col("id"))
						.agg(org.apache.spark.sql.functions.max(df.col("value")), typedCol,
								org.apache.spark.sql.functions.min(df.col("value")));
				datasetByDay.show();

				CassandraConnector.connect();
				Timestamp key = new Timestamp(cal.getTimeInMillis());
				String daytime = (cal.get(Calendar.DAY_OF_MONTH) < 10 ? "0" + cal.get(Calendar.DAY_OF_MONTH)
						: "" + cal.get(Calendar.DAY_OF_MONTH))
						+ ((cal.get(Calendar.MONTH) < 9) ? ("0" + (cal.get(Calendar.MONTH) + 1))
								: "" + (cal.get(Calendar.MONTH) + 1))
						+ cal.get(Calendar.YEAR) + "";
				List<Row> listRow = datasetByDay.toJavaRDD().collect();
				for (int i = 0; i < listRow.size(); i++) {
					String s = (String) listRow.get(i).getAs("id");
					if (s == null || s.length() < 10) {
						continue;
					}
					DataObject databyday = client.queryByDay(UUID.fromString(s), key);

					boolean isNew = false;
					if (databyday == null) {
						databyday = new DataObject();
						databyday.setId(UUID.fromString(s));
						databyday.setKey(key);
						databyday.setDaytime(daytime);
						isNew = true;
					}

					databyday.setAvg((Double) listRow.get(i).getAs(2));
					databyday.setMin((Double) listRow.get(i).getAs(3));
					databyday.setMax((Double) listRow.get(i).getAs(1));

					logger.info("data is " + databyday.toString()); // save to DB
					if (isNew) {
						client.insertToDB(databyday);
					} else {
						client.updateToDB(databyday);
					}
				}

				/* heart rate */
				if (args.length < 2)
					return;
				Dataset<HeartRateCurrent> dsHeartRate = sqlContext.read().json("hdfs://" + nameserviceId + args[1])
						.as(Encoders.bean(HeartRateCurrent.class));

				Dataset<Row> dsHeartRateAvg = dsHeartRate.where(dsHeartRate.col("time").gt(cal.getTimeInMillis()))
						.groupBy(dsHeartRate.col("id"))
						.agg(org.apache.spark.sql.functions.max(dsHeartRate.col("current_heart_rate")),
								org.apache.spark.sql.functions.avg(dsHeartRate.col("current_heart_rate")),
								org.apache.spark.sql.functions.min(dsHeartRate.col("current_heart_rate")));
				dsHeartRateAvg.show();
				List<Row> listHeartRateAvgRow = dsHeartRateAvg.toJavaRDD().collect();
				for (int i = 0; i < listHeartRateAvgRow.size(); i++) {
					HeartRateAvg heartRateAvg = client
							.searchHrAvgByDay(UUID.fromString((String) listHeartRateAvgRow.get(i).getAs("id")), key);
					boolean isNew = false;
					if (heartRateAvg == null) {
						heartRateAvg = new HeartRateAvg();
						heartRateAvg.setId(UUID.fromString((String) listHeartRateAvgRow.get(i).getAs("id")));
						heartRateAvg.setDay(key);
						heartRateAvg.setDaytime(daytime);
						isNew = true;
					}
					heartRateAvg.setHeart_rate_avg(listHeartRateAvgRow.get(i).getDouble(2));
					heartRateAvg.setHeart_rate_max(listHeartRateAvgRow.get(i).getDouble(1));
					heartRateAvg.setHeart_rate_min(listHeartRateAvgRow.get(i).getDouble(3));
					logger.info("data is " + heartRateAvg.toString()); // save to DB

					if (isNew) {
						client.insertToDB(heartRateAvg);
					} else {
						client.updateToDB(heartRateAvg);
					}
				}

				/* spO2 */
				if (args.length < 3)
					return;
				Dataset<Spo2Current> dsSPO2 = sqlContext.read().json("hdfs://" + nameserviceId + args[2])
						.as(Encoders.bean(Spo2Current.class));
				
				dsSPO2.show();
				Dataset<Row> dsSpo2Avg = dsSPO2.where(dsSPO2.col("time").gt(cal.getTimeInMillis()))
						.groupBy(dsSPO2.col("id")).agg(org.apache.spark.sql.functions.max(dsSPO2.col("spo2_current")),
								org.apache.spark.sql.functions.avg(dsSPO2.col("spo2_current")),
								org.apache.spark.sql.functions.min(dsSPO2.col("spo2_current")));

				List<Row> listSpo2AvgRow = dsSpo2Avg.toJavaRDD().collect();
				for (int i = 0; i < listSpo2AvgRow.size(); i++) {
					boolean isNew = false;
					UUID uuid = UUID.fromString((String) listSpo2AvgRow.get(i).getAs("id"));
					Spo2Avg spo2Avg = client.searchSpo2AvgByDay(uuid, key);
					if (spo2Avg == null) {
						isNew = true;
						spo2Avg = new Spo2Avg();
						spo2Avg.setId(uuid);
						spo2Avg.setDay(key);
						spo2Avg.setDaytime(daytime);
					}
					spo2Avg.setSpo2_max(listSpo2AvgRow.get(i).getDouble(1));
					spo2Avg.setSpo2_avg(listSpo2AvgRow.get(i).getDouble(2));
					spo2Avg.setSpo2_min(listSpo2AvgRow.get(i).getDouble(3));
					logger.info("data is " + spo2Avg.toString()); // save to DB

					if (isNew) {
						client.insertToDB(spo2Avg);
					} else {
						client.updateToDB(spo2Avg);
					}
				}

				/* blood_pressure */
				if (args.length < 4)
					return;
				Dataset<BloodPressureCurrent> dsBlood = sqlContext.read().json("hdfs://" + nameserviceId + args[3])
						.as(Encoders.bean(BloodPressureCurrent.class));

				Dataset<Row> dsBloodAvg = dsBlood.where(dsBlood.col("time").gt(cal.getTimeInMillis()))
						.groupBy(dsBlood.col("id")).agg(org.apache.spark.sql.functions.avg(dsBlood.col("high_value")),
								org.apache.spark.sql.functions.avg(dsBlood.col("low_value")));
				List<Row> listBloodAvgRow = dsBloodAvg.toJavaRDD().collect();
				for (int i = 0; i < listBloodAvgRow.size(); i++) {
					boolean isNew = false;
					UUID uuid = UUID.fromString((String) listBloodAvgRow.get(i).getAs("id"));
					BloodPressureAvg bloodAvg = client.searchBloodAvgByDay(uuid, key);
					if (bloodAvg == null) {
						isNew = true;
						bloodAvg = new BloodPressureAvg();
						bloodAvg.setId(uuid);
						bloodAvg.setDay(key);
						bloodAvg.setDaytime(daytime);
					}
					bloodAvg.setHigh_avg(listBloodAvgRow.get(i).getDouble(1));
					bloodAvg.setLow_avg(listBloodAvgRow.get(i).getDouble(2));
					logger.info("data is " + bloodAvg.toString()); // save to DB

					if (isNew) {
						client.insertToDB(bloodAvg);
					} else {
						client.updateToDB(bloodAvg);
					}
				}

				/*
				 * cal.set(Calendar.DAY_OF_MONTH, 1); Dataset<Row> datasetByMonth =
				 * df.where(df.col("upTime").gt(cal.getTimeInMillis())).agg(
				 * org.apache.spark.sql.functions.max(df.col("value")),
				 * org.apache.spark.sql.functions.avg(df.col("value")),
				 * org.apache.spark.sql.functions.min(df.col("value"))); datasetByMonth.show();
				 * 
				 * key = new Timestamp(cal.getTimeInMillis()); DataObject dataMonth =
				 * client.queryByDay(key); isNew = false; if (dataMonth == null) { dataMonth =
				 * new DataObject(); dataMonth.setKey(key);
				 * dataMonth.setDaytime(cal.toString()); String daytime =
				 * ((cal.get(Calendar.MONTH) < 9) ? ("0" + (cal.get(Calendar.MONTH) + 1)) : "" +
				 * (cal.get(Calendar.MONTH) + 1)) + cal.get(Calendar.YEAR) + "";
				 * dataMonth.setDaytime(daytime); isNew = true; }
				 * 
				 * dataMonth.setAvg(datasetByMonth.select("avg(value)").as(Encoders.DOUBLE()).
				 * toJavaRDD().collect().get(0));
				 * dataMonth.setMin(datasetByMonth.select("min(value)").as(Encoders.DOUBLE()).
				 * toJavaRDD().collect().get(0));
				 * dataMonth.setMax(datasetByMonth.select("max(value)").as(Encoders.DOUBLE()).
				 * toJavaRDD().collect().get(0));
				 * 
				 * logger.debug("data is " + dataMonth.toString()); // save to DB if (isNew) {
				 * client.insertToDB(dataMonth); } else { client.updateToDB(dataMonth); }
				 */

				CassandraConnector.close();
				logger.info("Program END: time to execute:  "
						+ (Calendar.getInstance().getTimeInMillis() - startTime.getTimeInMillis()) + " ms");
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		/*
		 * //Create a SparkContext to initialize SparkConf conf = new
		 * SparkConf().setMaster("spark://10.55.123.60:7077").setAppName("Word Count");
		 * 
		 * // Create a Java version of the Spark Context JavaSparkContext sc = new
		 * JavaSparkContext(conf);
		 * 
		 * // Load the text into a Spark RDD, which is a distributed representation of
		 * each line of text JavaRDD<String> textFile =
		 * sc.textFile("src/main/resources/abc.txt"); JavaPairRDD<String, Integer>
		 * counts = textFile.flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
		 * .mapToPair(word -> new Tuple2<>(word, 1)) .reduceByKey((a, b) -> a + b);
		 * counts.foreach(p -> System.out.println(p));
		 * System.out.println("Total words: " + counts.count());
		 * counts.saveAsTextFile("/tmp/shakespeareWordCount");
		 */
	}

	public static class DemoReducer extends Reducer<Text, LongWritable, LongWritable, Text> {
		final static Logger loggerReduce = Logger.getLogger(DemoReducer.class);

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

		}
	}

	public static class DemoMapper extends Mapper<Object, Text, Text, LongWritable> {
		final static Logger loggerMap = Logger.getLogger(DemoMapper.class);
		TreeMap<String, Double> treeResult = new TreeMap<String, Double>();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// key: line number
			// value: line value
			loggerMap.info(value);
		}
	}
}
