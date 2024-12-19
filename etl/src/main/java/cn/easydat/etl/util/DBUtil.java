package cn.easydat.etl.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;

import cn.easydat.etl.entity.parameter.JobParameterJdbc;

public class DBUtil {

	public static void loadDriverClass(String driver) {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("数据库驱动加载错误,driver: " + driver, e);
		}
	}

	public static Connection getConnection(final JobParameterJdbc jobParameterJdbc) {
		try {
			return RetryUtil.executeWithRetry(new Callable<Connection>() {
				@Override
				public Connection call() throws Exception {
					return DBUtil.connect(jobParameterJdbc);
				}
			}, 9, 1000L, true);
		} catch (Exception e) {
			throw new RuntimeException("数据库连接失败. 因为根据您配置的连接信息:获取数据库连接失败. 请检查您的配置并作出修改. jdbc:" + jobParameterJdbc, e);
		}
	}

//	public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout) throws SQLException {
//		conn.setAutoCommit(false);
//		Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//		stmt.setFetchSize(fetchSize);
//		stmt.setQueryTimeout(queryTimeout);
//		return query(stmt, sql);
//	}

	public static ResultSet query(Statement stmt, String sql) throws SQLException {
		return stmt.executeQuery(sql);
	}

//	public static ResultSet query(Connection conn, String sql) throws SQLException {
//		Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//		// 默认3600 seconds
//		stmt.setQueryTimeout(172800);
//		return query(stmt, sql);
//	}

	public static void executeSqlWithoutResultSet(Statement stmt, String sql) throws SQLException {
		stmt.execute(sql);
	}

	public static void closeResultSet(ResultSet rs) {
		try {
			if (null != rs) {
				Statement stmt = rs.getStatement();
				if (null != stmt) {
					stmt.close();
					stmt = null;
				}
				rs.close();
			}
			rs = null;
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn) {
		if (null != rs) {
			try {
				rs.close();
			} catch (SQLException unused) {
			}
		}

		if (null != stmt) {
			try {
				stmt.close();
			} catch (SQLException unused) {
			}
		}

		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException unused) {
			}
		}
	}

	public static void closeDBResources(Statement stmt, Connection conn) {
		closeDBResources(null, stmt, conn);
	}

	private static synchronized Connection connect(JobParameterJdbc jobParameterJdbc) {
		try {
			Class.forName(jobParameterJdbc.getDriver());
			DriverManager.setLoginTimeout(15);

			Properties prop = new Properties();
			prop.put("user", jobParameterJdbc.getUsername());
			prop.put("password", jobParameterJdbc.getPassword());
			
			if (jobParameterJdbc.getDriver().contains("mysql")) {
				prop.put("useCursorFetch", true);
			}

			return DriverManager.getConnection(jobParameterJdbc.getJdbcUrl(), prop);
		} catch (Exception e) {
			throw new RuntimeException(jobParameterJdbc.toString());
		}
	}
	
	public static String getFields(String[] columns) {
		String fields = "";
		for (int i = 0; i < columns.length - 1; i++) {
			String column = columns[i];
			fields += column + ",";
		}
		fields += columns[columns.length - 1];
		return fields;
	}

}
