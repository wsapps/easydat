package cn.easydat.etl.job.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.entity.MetaData;
import cn.easydat.etl.process.consumer.CustomTransform;
import cn.easydat.etl.util.DBUtil;

public class Consumer {

	private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
	private static final Integer ERROR_RUN_STATUS = -1;

	private String readSql;
	private String writeSql;
	private String deleteSql;
	private JobParameter jobParameter;

	private BlockingQueue<Object[]> dataQueue;
	private List<MetaData> metaDatas;
	private boolean readRun;
	private Connection writeConn;
	private List<Object[]> writeDate;
	private boolean error;
	private int writeNum;

	public Consumer(String readSql, String writeSql, String deleteSql, JobParameter jobParameter) {
		super();
		this.readSql = readSql;
		this.writeSql = writeSql;
		this.deleteSql = deleteSql;
		this.jobParameter = jobParameter;

		this.dataQueue = new LinkedBlockingQueue<>(this.jobParameter.getSetting().getMaxNumOfChannel());
		this.writeDate = new ArrayList<Object[]>(this.jobParameter.getWriter().getBatchSize());
		this.writeNum = 0;
	}

	public boolean run(Long id, Integer runStatus) {
		Thread.currentThread().setName("consumer-" + id);
		readRun = true;
		error = false;

		if (ERROR_RUN_STATUS.equals(runStatus)) {
			if (null != deleteSql) {
				try (Connection conn = getWriteConn(); Statement stmt = conn.createStatement()) {
					LOG.info("delete start sql :" + deleteSql);
					stmt.execute(deleteSql);
					LOG.info("delete end sql :" + deleteSql);
				} catch (Exception e) {
					LOG.error("run,sql:" + deleteSql, e);
					error = true;
				}
			}
		}
		
		if (!error) {
			Thread thread = new Thread(() -> {
				try {
					write();
				} catch (Exception e) {
					LOG.error("run,sql:" + readSql, e);
					error = true;
				}
			});
			thread.setName("consumer-write-" + id);
			thread.start();

			try {
				read();
			} catch (Exception e) {
				LOG.error("run,sql:" + readSql, e);
				error = true;
			}
			
			if (!error) {
				while (!this.dataQueue.isEmpty()) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						LOG.error("run,sql:" + readSql, e);
					}
				}

				try {
					writeEnd();
				} catch (SQLException e) {
					error = true;
					throw new RuntimeException(e);
				}
			}

		}

		return error;
	}

	private void read() {
		try (Connection readerConn = DBUtil.getConnection(jobParameter.getReader().getJdbc()); Statement stmt = createReadStatement(readerConn); ResultSet rs = stmt.executeQuery(readSql)) {
			List<MetaData> metaDataList = getMetaDatas(rs);
			int fieldSize = metaDataList.size();

			while (rs.next()) {
				
				if (error) {
					break;
				}

				Object[] data = new Object[fieldSize];
				for (int i = 0; i < fieldSize; i++) {
					data[i] = rs.getObject(i + 1);
				}

				dataQueueOffer(data);
			}

			readRun = false;
		} catch (Exception e) {
			error = true;
			throw new RuntimeException(e);
		}
	}

	private void write() {
		while (this.readRun || (!this.readRun && !this.dataQueue.isEmpty())) {

			try {
				Object[] data = dataQueuePoll();
				transformHandler(data);
			} catch (SQLException e) {
				//LOG.error("", e);
				
				error = true;
				throw new RuntimeException(e);
			}
		}

		Connection conn = getWriteConn();
		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException e) {
				LOG.error("", e);
				error = true;
				throw new RuntimeException(e);
			}
		}

	}

	private Object[] transformHandler(Object[] data) throws SQLException {
		CustomTransform transform = jobParameter.getCustomTransform();
		Object[] dataPending = data;
		if (null != transform) {
			dataPending = transform.handle(dataPending);
		}

		if (null != dataPending && dataPending.length > 0) {
			writeHandler(dataPending);
		}
		return data;
	}

	private void writeHandler(Object[] row) throws SQLException {
		writeDate.add(row);

		if (this.writeDate.size() == jobParameter.getWriter().getBatchSize()) {
			Connection conn = null;
			PreparedStatement ps = null;
			try {
				conn = getWriteConn();
				ps = conn.prepareStatement(writeSql);
				for (Object[] data : this.writeDate) {
					for (int i = 0; i < data.length; i++) {
						ps.setObject(i + 1, data[i]);
					}
					ps.addBatch();
				}
				ps.executeBatch();
				ps.clearBatch();
				conn.commit();

				this.writeNum += this.writeDate.size();
				this.writeDate.clear();
			} catch (SQLException e) {
				conn.rollback();
				
				if (null != conn) {
					conn.close();
				}
				this.error = true;
				throw new RuntimeException(e);
			} finally {
				if (null != ps) {
					ps.close();
				}
			}

			
		}
	}

	private void writeEnd() throws SQLException {

		if (!this.writeDate.isEmpty()) {
			Connection conn = null;
			PreparedStatement ps = null;
			
			try {
				conn = getWriteConn();
				ps = conn.prepareStatement(writeSql);
				for (Object[] data : this.writeDate) {
					for (int i = 0; i < data.length; i++) {
						ps.setObject(i + 1, data[i]);
					}
					ps.addBatch();
				}
				ps.executeBatch();
				ps.clearBatch();
				conn.commit();

				this.writeNum += this.writeDate.size();
				this.writeDate.clear();

				
			} catch (SQLException e) {
				conn.rollback();
				if (null != conn) {
					conn.close();
				}
				this.error = true;
				throw new RuntimeException(e);
			} finally {
				if (null != ps) {
					ps.close();
				}
			}
		}
	}

	private Connection getWriteConn() {
		try {
			if (null == this.writeConn) {
				this.writeConn = DBUtil.getConnection(jobParameter.getWriter().getJdbc());
				this.writeConn.setAutoCommit(false);
			} else {
				try {
					boolean isValid = this.writeConn.isValid(5);
					if (!isValid) {
						this.writeConn = DBUtil.getConnection(jobParameter.getWriter().getJdbc());
						this.writeConn.setAutoCommit(false);
					}
				} catch (SQLException e) {
					this.writeConn = DBUtil.getConnection(jobParameter.getWriter().getJdbc());
					this.writeConn.setAutoCommit(false);
				}
			}
		} catch (SQLException e) {
			LOG.error("", e);
		}

		return this.writeConn;
	}

	private Statement createReadStatement(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		return stmt;
	}

	private List<MetaData> getMetaDatas(ResultSet rs) {
		if (null == metaDatas) {
			synchronized (this) {
				if (null == metaDatas) {
					loadMetaData(rs);
				}
			}
		}
		return metaDatas;
	}

	private void loadMetaData(ResultSet rs) {

		if (null != metaDatas) {
			return;
		}

		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();

			List<MetaData> metaDataList = new ArrayList<>(columnCount);

			for (int i = 1; i <= columnCount; i++) {
				MetaData md = new MetaData(metaData.getColumnLabel(i), metaData.getColumnName(i), metaData.getColumnTypeName(i), metaData.getColumnType(i));
				metaDataList.add(md);
			}

			this.metaDatas = metaDataList;
		} catch (SQLException e) {
			LOG.error("", e);
			throw new RuntimeException(e);
		}
	}

	private void dataQueueOffer(Object[] data) {
		int i = 0;
		boolean flag = false;
		while (!flag) {
			
			if (error) {
				break;
			}

			try {
				flag = this.dataQueue.offer(data, 200, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				LOG.error("dataQueuePut error", e);
				Thread.currentThread().interrupt();
			}
			i++;

			if (i % 100 == 0) {
				LOG.warn("dataQueuePut wait " + i + ", dataQueue size:" + this.dataQueue.size() + ", writeNum:" + this.writeNum);
			}
		}
	}

	private Object[] dataQueuePoll() {
		Object[] data = null;
		try {
			data = this.dataQueue.poll(200, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			LOG.error("dataQueueTake error", e);
			Thread.currentThread().interrupt();
		}
		return data;
	}

	public int getWriteNum() {
		return writeNum;
	}

}
