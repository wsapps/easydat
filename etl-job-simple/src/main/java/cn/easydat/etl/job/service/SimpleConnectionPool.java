package cn.easydat.etl.job.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.parameter.JobParameterJdbc;

public class SimpleConnectionPool {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConnectionPool.class);
	
	private String url;
	private String user;
	private String password;
	private int maxSize;
	private BlockingQueue<Connection> pool;
	
	public SimpleConnectionPool(JobParameterJdbc jobParameterJdbc, int initialSize, int maxSize) throws SQLException {
		this.url = jobParameterJdbc.getJdbcUrl();
		this.user = jobParameterJdbc.getUsername();
		this.password = jobParameterJdbc.getPassword();
		this.maxSize = maxSize;
		this.pool = new LinkedBlockingQueue<>(maxSize);

//		// 初始化连接池
//		for (int i = 0; i < initialSize; i++) {
//			pool.offer(createConnection());
//		}
	}

	public SimpleConnectionPool(String url, String user, String password, int initialSize, int maxSize) throws SQLException {
		this.url = url;
		this.user = user;
		this.password = password;
		this.maxSize = maxSize;
		this.pool = new LinkedBlockingQueue<>(maxSize);

		// 初始化连接池
		for (int i = 0; i < initialSize; i++) {
			pool.offer(createConnection());
		}
	}

	// 创建新连接
	private Connection createConnection() throws SQLException {
		LOGGER.info("createConnection");
		return DriverManager.getConnection(url, user, password);
	}

	// 获取连接
	public Connection getConnection() throws SQLException {
		Connection conn = pool.poll();
		
		if (conn == null && pool.size() < maxSize) {
			return createConnection(); // 池子为空且未达到最大容量时创建新连接
		}
		if (conn == null) {
			throw new SQLException("No available connection in the pool.");
		}
		
		boolean isValid = conn.isValid(5);
		
		if (!isValid) {
			if (null != conn) {
				conn.close();
			}
			return createConnection();
		}
		return conn;
	}

	// 归还连接
	public void releaseConnection(Connection conn) {
		if (conn != null) {
			try {
				if (!conn.getAutoCommit()) {
					conn.setAutoCommit(true);
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			pool.offer(conn);
		}
	}

	// 关闭连接池
	public void shutdown() throws SQLException {
		for (Connection conn : pool) {
			conn.close();
		}
		pool.clear();
	}
}
