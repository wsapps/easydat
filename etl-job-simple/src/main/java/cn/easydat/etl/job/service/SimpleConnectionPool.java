package cn.easydat.etl.job.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.parameter.JobParameterJdbc;

public class SimpleConnectionPool {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConnectionPool.class);
	private static final String CREATE_TIME = "createTime";
	private static final long MAX_RUM_TIME_MILLIS = 60 * 20 * 1000;

	private int maxSize;
	private Map<String, BlockingQueue<Connection>> connPool;
	private AtomicInteger currentSize = new AtomicInteger(0);

	public SimpleConnectionPool(int maxSize) {
		this.maxSize = maxSize;
		this.connPool = new ConcurrentHashMap<String, BlockingQueue<Connection>>();

	}

	// 创建新连接
	private Connection createConnection(JobParameterJdbc jdbc) throws SQLException {
		LOGGER.info("createConnection start, jdbcUrl:{},username:{},currentSize:{}", getJdbcUrlIP(jdbc), jdbc.getUsername(), currentSize.get());
		Connection conn = DriverManager.getConnection(jdbc.getJdbcUrl(), jdbc.getUsername(), jdbc.getPassword());
		conn.setClientInfo(CREATE_TIME, System.currentTimeMillis() + "");

		LOGGER.info("createConnection end, jdbcUrl:{},username:{},currentSize:{}", getJdbcUrlIP(jdbc), jdbc.getUsername(), currentSize.get());
		return conn;
	}

	// 获取连接
	public Connection getConnection(JobParameterJdbc jdbc) throws SQLException {
		BlockingQueue<Connection> poolQueue = getPoolQueue(jdbc);
		LOGGER.info("getConnection start, jdbcUrl:{},username:{},currentSize:{},connPoolSize:{}", getJdbcUrlIP(jdbc), jdbc.getUsername(), currentSize.get(), poolQueue.size());
		Connection conn = poolQueue.poll();

		if (conn == null) {

			if (currentSize.incrementAndGet() < maxSize) {
				conn = createConnection(jdbc);
			} else {
				currentSize.decrementAndGet();
			}
		}

		if (conn == null) {
			throw new SQLException("No available connection in the pool.");
		}

		boolean isValid = conn.isValid(5);

		if (!isValid) {
			if (null != conn) {
				conn.close();
			}

			conn = createConnection(jdbc);
		}
		return conn;
	}

	// 归还连接
	public void releaseConnection(JobParameterJdbc jdbc, Connection conn) {
		if (conn != null) {
			boolean discard = false;
			try {
				String createTimeStr = conn.getClientInfo(CREATE_TIME);
				if (null != createTimeStr) {
					long createTime = Long.parseLong(createTimeStr);
					long currentTime = System.currentTimeMillis();
					long runTime = currentTime - createTime;
					if (runTime >= MAX_RUM_TIME_MILLIS) {
						discard = true;
						currentSize.decrementAndGet();
						LOGGER.info("releaseConnection discard, jdbcUrl:{},username:{},createTime:{},currentTime:{}", getJdbcUrlIP(jdbc), jdbc.getUsername(), createTime, currentTime);
					}
				}

				if (!conn.getAutoCommit()) {
					conn.setAutoCommit(true);
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}

			if (!discard) {
				BlockingQueue<Connection> poolQueue = getPoolQueue(jdbc);
				poolQueue.offer(conn);
			} else {
				if (null != conn) {
					try {
						conn.close();
					} catch (SQLException e) {
						LOGGER.info("", e);
					}
				}
			}
		}
	}
	
	private String getJdbcUrlIP(JobParameterJdbc jdbc) {
		String url = jdbc.getJdbcUrl().substring(13);
		String[] urlArr = url.split(":");
		return urlArr[0];
	}

	// 关闭连接池
	public void shutdown() throws SQLException {

		for (Entry<String, BlockingQueue<Connection>> e : connPool.entrySet()) {
			for (Connection conn : e.getValue()) {
				conn.close();
			}
			e.getValue().clear();
		}
	}

	private String getKey(JobParameterJdbc jdbc) {
		return jdbc.getJdbcUrl() + "~" + jdbc.getUsername();
	}

	private BlockingQueue<Connection> getPoolQueue(JobParameterJdbc jdbc) {
		String key = getKey(jdbc);

		if (!connPool.containsKey(key)) {
			synchronized (this) {
				if (!connPool.containsKey(key)) {
					connPool.put(key, new LinkedBlockingQueue<Connection>(maxSize));
				}
			}
		}

		return connPool.get(key);
	}
}
