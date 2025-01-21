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
	private PreparedStatement writePS;
	private boolean error;
	private int writeNum;
	private Long id;
	private SimpleConnectionPool readPool;
	private SimpleConnectionPool writePool;

	public Consumer(String readSql, String writeSql, String deleteSql, JobParameter jobParameter, SimpleConnectionPool readPool, SimpleConnectionPool writePool) {
		super();
		this.readSql = readSql;
		this.writeSql = writeSql;
		this.deleteSql = deleteSql;
		this.jobParameter = jobParameter;
		this.readPool = readPool;
		this.writePool = writePool;

		this.dataQueue = new LinkedBlockingQueue<>(this.jobParameter.getSetting().getMaxNumOfChannel());
		this.writeNum = 0;
	}

	public boolean run(Long id, Integer runStatus) {
//		Thread.currentThread().setName("consumer-" + id);
		this.id = id;
		readRun = true;
		error = false;

		if (ERROR_RUN_STATUS.equals(runStatus)) {
			if (null != deleteSql) {
				try (Connection conn = writePool.getConnection(); Statement stmt = conn.createStatement()) {
					LOG.info("delete start sql :" + deleteSql);
					stmt.execute(deleteSql);
					LOG.info("delete end sql :" + deleteSql);
					writePool.releaseConnection(conn);
				} catch (Exception e) {
					LOG.error("run,sql:" + deleteSql, e);
					error = true;
				}
			}
		}

		if (!error) {
			Thread thread = new Thread(() -> {
				try {
					LOG.info("write getConnection start");
					this.writeConn = writePool.getConnection();
					this.writeConn.setAutoCommit(false);
					this.writePS = this.writeConn.prepareStatement(writeSql);
					LOG.info("write getConnection end");

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

			while (!this.dataQueue.isEmpty()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					LOG.error("run,sql:" + readSql, e);
				}
			}

			if (error) {
				throw new RuntimeException();
			}

		}

		return error;
	}

	private void read() {
		LOG.info("read getConnection start");
		try (Connection readerConn = readPool.getConnection(); Statement stmt = createReadStatement(readerConn); ResultSet rs = stmt.executeQuery(readSql)) {
			LOG.info("read getConnection end");
			List<MetaData> metaDataList = getMetaDatas(rs);
			int fieldSize = metaDataList.size();

			while (rs.next()) {

				if (error) {
					this.dataQueue.clear();
					break;
				}

				Object[] data = new Object[fieldSize];
				for (int i = 0; i < fieldSize; i++) {
					data[i] = rs.getObject(i + 1);
				}

				dataQueueOffer(data);
			}

			readRun = false;
			readPool.releaseConnection(readerConn);
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
				// LOG.error("", e);

				error = true;
				throw new RuntimeException(e);
			}
		}

		Connection conn = this.writeConn;
		PreparedStatement ps = this.writePS;

		if (null != ps) {
			try {
				ps.executeBatch();
				ps.clearBatch();
				ps.close();
			} catch (SQLException e) {
				LOG.error("", e);
				error = true;
				throw new RuntimeException(e);
			}
		}

		if (null != conn) {
			try {
				conn.commit();
				conn.close();
			} catch (SQLException e) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					
				} finally {
					if (null != conn) {
						try {
							conn.close();
						} catch (SQLException e1) {
							LOG.error("", e1);
						}
					}
				}
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
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = this.writeConn;
			ps = this.writePS;
			for (int i = 0; i < row.length; i++) {
				ps.setObject(i + 1, row[i]);
			}
			ps.addBatch();
			this.writeNum++;
			
//			if (this.writeNum == 1 || this.writeNum % 100 == 0) {
//				LOG.info("write addBatch, writeNum:" + writeNum);
//			}

			if (this.writeNum % jobParameter.getWriter().getBatchSize() == 0) {
//				LOG.info("write executeBatch start");
				ps.executeBatch();
//				LOG.info("write executeBatch end");
				ps.clearBatch();
//				LOG.info("write clearBatch end");
				conn.commit();
//				LOG.info("write commit end");

			}

			if (this.writeNum % (jobParameter.getWriter().getBatchSize() * 10) == 0) {
				LOG.info("commit,id:" + id + ", writeNum:" + writeNum);
			}
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				
			} finally {
				if (null != conn) {
					conn.close();
				}
			}
			
			this.error = true;
			throw new RuntimeException(e);
		}

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
		long timeout = 50;
		long printExeNum = (1000 / timeout) * 30;
		
		while (!flag) {

			if (error) {
				this.dataQueue.clear();
				break;
			}

			try {
				flag = this.dataQueue.offer(data, timeout, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				LOG.error("dataQueuePut error", e);
				Thread.currentThread().interrupt();
			}
			i++;

			if (i % printExeNum == 0) {
				LOG.warn(id + "-dataQueuePut wait " + i + ", dataQueue size:" + this.dataQueue.size() + ", writeNum:" + this.writeNum);
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
