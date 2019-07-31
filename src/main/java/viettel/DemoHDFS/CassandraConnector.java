package viettel.DemoHDFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import viettel.DemoHDFS.BloodPressure.Entity.BloodPressureAvg;
import viettel.DemoHDFS.HeartRate.Entity.HeartRateAvg;
import viettel.DemoHDFS.SPO2.Entity.Spo2Avg;
import viettel.DemoHDFS.Tempature.Entity.DataObject;

public class CassandraConnector {

	final static Logger logger = Logger.getLogger(CassandraConnector.class);
	private static CassandraConnector instance = new CassandraConnector();
	private static Cluster cluster;
	private static Session session;
	private static String node;
	private static int port;
	private static String username;
	private static String pwd;
	private static String keyspace;

	static {
		try {
			Properties pro = new Properties();
			pro.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("cassandra.properties"));
			node = pro.getProperty("cassandra.contactpoints");
			port = Integer.parseInt(pro.getProperty("cassandra.port"));
			username = pro.getProperty("cassandra.user");
			pwd = pro.getProperty("cassandra.password");
			keyspace = pro.getProperty("cassandra.keyspace");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void connect() {
		cluster = Cluster.builder().addContactPoint(node).withPort(port).withCredentials(username, pwd).build();
		//cluster.getConfiguration().getCodecRegistry().register(InstantCodec.instance);
		final Metadata metadata = cluster.getMetadata();
		logger.info("Connected to cluster: " + metadata.getClusterName());
		for (final Host host : metadata.getAllHosts()) {
			logger.info("Datacenter: " + host.getDatacenter() + "; Host: " + host.getAddress() + "; Rack: "
					+ host.getRack());
		}
		session = cluster.connect(keyspace);

	}

	public static void close() {
		if (session != null)
			session.close();
		if (cluster != null)
			cluster.close();
	}

	public DataObject queryByDay(UUID id, Timestamp key) {
		ResultSet rs = session.execute(
				"SELECT * FROM temp_avg_day WHERE day = " + key.getTime() + " and id=" + id + " ALLOW FILTERING");
		Row row = rs.one();
		if (row == null) {
			return null;
		} else {
			DataObject data = new DataObject();
			data.setId(row.getUUID("id"));
			data.setKey(new Timestamp(row.getTimestamp("day").getTime()));
			data.setDaytime(row.getString("daytime"));
			return data;
		}
	}

	public void insertToDB(DataObject dataByDay) {
		String querry = "INSERT INTO temp_avg_day (id, day,daytime,avg_temp_day,max_temp,min_temp)" + "VALUES("
				+ dataByDay.getId() + "," + dataByDay.getKey().getTime() + ",'" + dataByDay.getDaytime() + "',"
				+ dataByDay.getAvg() + "," + dataByDay.getMax() + "," + dataByDay.getMin() + ");";
		logger.info("Execute Querry: " + querry);
		try {
			session.execute(querry);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void updateToDB(DataObject databyday) {
		String querry = "UPDATE temp_avg_day set avg_temp_day=" + databyday.getAvg() + ",max_temp=" + databyday.getMax()
				+ ",min_temp=" + databyday.getMin() + " WHERE id=" + databyday.getId() + " and day = "
				+ databyday.getKey().getTime() + ";";
		logger.info("Execute Querry: " + querry);
		try {
			session.execute(querry);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static CassandraConnector getInstance() {
		return instance;
	}

	public HeartRateAvg searchHrAvgByDay(UUID uuid, Timestamp key) {
		PreparedStatement prepared = session.prepare("select * from heart_rate_avg where id = ? and day = ?");
		BoundStatement bound = prepared.bind(uuid, key);
		ResultSet rs = session.execute(bound);
		Row row = rs.one();
		if (row == null) {
			return null;
		} else {
			HeartRateAvg data = new HeartRateAvg();
			data.setId(row.getUUID("id"));
			data.setDay(new Timestamp(row.getTimestamp("day").getTime()));
			data.setDaytime(row.getString("daytime"));
			return data;
		}
	}

	public void insertToDB(HeartRateAvg heartRateAvg) {
		try {
			PreparedStatement prepared = session.prepare(
					"insert into heart_rate_avg (id, day, heart_rate_avg, heart_rate_max, heart_rate_min, daytime) values(?,?,?,?,?,?)");
			BoundStatement bound = prepared.bind(heartRateAvg.getId(), heartRateAvg.getDay(),
					heartRateAvg.getHeart_rate_avg(), heartRateAvg.getHeart_rate_max(),
					heartRateAvg.getHeart_rate_min(), heartRateAvg.getDaytime());
			session.execute(bound);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void updateToDB(HeartRateAvg heartRateAvg) {
		try {
			PreparedStatement prepared = session.prepare(
					"update heart_rate_avg set heart_rate_avg = ?, heart_rate_max = ?, heart_rate_min = ? where id=? and day=?");
			BoundStatement bound = prepared.bind(heartRateAvg.getHeart_rate_avg(), heartRateAvg.getHeart_rate_max(),
					heartRateAvg.getHeart_rate_min(), heartRateAvg.getId(), heartRateAvg.getDay());
			session.execute(bound);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Spo2Avg searchSpo2AvgByDay(UUID uuid, Timestamp key) {
		PreparedStatement prepared = session.prepare("select * from spo2_avg where id = ? and day = ?");
		BoundStatement bound = prepared.bind(uuid, key);
		ResultSet rs = session.execute(bound);
		Row row = rs.one();
		if (row == null) {
			return null;
		} else {
			Spo2Avg data = new Spo2Avg();
			data.setId(row.getUUID("id"));
			data.setDay(new Timestamp(row.getTimestamp("day").getTime()));
			data.setDaytime(row.getString("daytime"));
			return data;
		}
	}

	public void insertToDB(Spo2Avg spo2Avg) {
		try {
			PreparedStatement prepared = session.prepare(
					"insert into spo2_avg (id, day, spo2_avg, spo2_max, spo2_min, daytime) values (?,?,?,?,?,?)");
			BoundStatement bound = prepared.bind(spo2Avg.getId(), spo2Avg.getDay(), spo2Avg.getSpo2_avg(),
					spo2Avg.getSpo2_max(), spo2Avg.getSpo2_min(), spo2Avg.getDaytime());
			session.execute(bound);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void updateToDB(Spo2Avg spo2Avg) {
		try {
			PreparedStatement prepared = session
					.prepare("update spo2_avg set spo2_avg = ?, spo2_max = ?, spo2_min = ? where id=? and day=?");
			BoundStatement bound = prepared.bind(spo2Avg.getSpo2_avg(), spo2Avg.getSpo2_max(), spo2Avg.getSpo2_min(),
					spo2Avg.getId(), spo2Avg.getDay());
			session.execute(bound);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public BloodPressureAvg searchBloodAvgByDay(UUID uuid, Timestamp key) {
		PreparedStatement prepared = session.prepare("select * from blood_pressure_avg where id = ? and day = ?");
		BoundStatement bound = prepared.bind(uuid, key);
		ResultSet rs = session.execute(bound);
		Row row = rs.one();
		if (row == null) {
			return null;
		} else {
			BloodPressureAvg data = new BloodPressureAvg();
			data.setId(row.getUUID("id"));
			data.setDay(new Timestamp(row.getTimestamp("day").getTime()));
			data.setDaytime(row.getString("daytime"));
			return data;
		}
	}

	public void insertToDB(BloodPressureAvg bloodAvg) {
		try {
			PreparedStatement prepared = session
					.prepare("insert into blood_pressure_avg (id, day, high_avg, low_avg, daytime) values (?,?,?,?,?)");
			BoundStatement bound = prepared.bind(bloodAvg.getId(), bloodAvg.getDay(), bloodAvg.getHigh_avg(),
					bloodAvg.getLow_avg(), bloodAvg.getDaytime());
			session.execute(bound);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void updateToDB(BloodPressureAvg bloodAvg) {
		try {
			PreparedStatement prepared = session
					.prepare("update blood_pressure_avg set high_avg = ?, low_avg = ? where id=? and day=?");
			BoundStatement bound = prepared.bind(bloodAvg.getHigh_avg(), bloodAvg.getLow_avg(), bloodAvg.getId(),
					bloodAvg.getDay());
			session.execute(bound);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
