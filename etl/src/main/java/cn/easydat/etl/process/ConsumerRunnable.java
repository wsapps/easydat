package cn.easydat.etl.process;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.entity.MetaData;
import cn.easydat.etl.entity.parameter.JobParameterWriter;
import cn.easydat.etl.util.DBUtil;

public class ConsumerRunnable implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(ConsumerRunnable.class);

	private JobParameter parameter;
	private String readerSql;
	private int threadNo;

	private List<MetaData> metaDatas;
	private Queue<List<Object>> dataQueue;

	private boolean readerFinish;
	
//	private Connection writerConn;
//	private PreparedStatement writerPreparedStatement;
	
	private int writerThreadNum;
	private List<Connection> writerConns;
	private String writeSql;
//	private List<PreparedStatement> writerPreparedStatements;

	private AtomicLong readerNum;
	private AtomicLong writerMonitorNum;

	public ConsumerRunnable(JobParameter parameter, String readerSql, int threadNo) {
		super();
		this.parameter = parameter;
		this.readerSql = readerSql;
		this.threadNo = threadNo;
		this.readerFinish = false;
		this.writerThreadNum = 2;

		this.dataQueue = new LinkedBlockingQueue<List<Object>>(parameter.getSetting().getMaxNumOfChannel());
		this.readerNum = new AtomicLong(1);
		this.writerMonitorNum = new AtomicLong(1);
		
		writerInit();
	}

	@Override
	public void run() {
		ExecutorService executorService = Executors.newFixedThreadPool(2 + writerThreadNum);

		executorService.submit(() -> {
			monitor();
		});

		executorService.submit(() -> {
			readerConsumerHandler();
		});

		for (int i = 0; i < writerThreadNum; i++) {
			int no = i;
			executorService.submit(() -> {
				while (true) {
					if (readerFinish && dataQueue.isEmpty()) {
						break;
					}
					handle(no);
				}
			});
		}

		executorService.shutdown();

		while (!executorService.isTerminated()) {
			try {
				executorService.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.error("", e);
			}
		}
		
		close();
	}

	private void readerConsumerHandler() {
		Connection readerConn = null;
		Statement stmt = null;
		ResultSet rs = null;

		LOG.info("threadNo:{}, readerSql:{}", threadNo, readerSql);

		try {
			readerConn = DBUtil.getConnection(parameter.getReader().getJdbc());
			stmt = readerConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			rs = stmt.executeQuery(readerSql);
			List<MetaData> metaDataList = getMetaDatas(rs);

			while (rs.next()) {
				readerNum.addAndGet(1);

				int fieldSize = metaDataList.size();
				List<Object> data = new ArrayList<Object>(fieldSize);
				for (int i = 1; i <= fieldSize; i++) {
					data.add(rs.getObject(i));
				}

				while (true) {
					boolean isOk = dataQueue.offer(data);

					if (isOk) {
						break;
					} else {
						Thread.sleep(200);
					}
				}
			}

		} catch (Exception e) {
			LOG.error("", e);
		} finally {
			DBUtil.closeDBResources(rs, stmt, readerConn);
			readerFinish = true;
		}
	}
	
	private void writerInit() {
		JobParameterWriter writer = parameter.getWriter();
		String fields = DBUtil.getFields(writer.getColumn());
		this.writeSql = String.format("INSERT INTO %s (%s) VALUES(%s)", writer.getTableName(), fields, values(writer.getColumn().length));
		
		writerConns = new ArrayList<Connection>(writerThreadNum);
//		writerPreparedStatements = new ArrayList<PreparedStatement>(writerThreadNum);

		for (int i = 0; i < writerThreadNum; i++) {
			try {
				Connection writerConn = DBUtil.getConnection(writer.getJdbc());
				writerConn.setAutoCommit(false);
//				PreparedStatement writerPreparedStatement = writerConn.prepareStatement(sql);
				writerConns.add(writerConn);
//				writerPreparedStatements.add(writerPreparedStatement);
			} catch (SQLException e) {
				LOG.error("", e);
			}
		}

	}

	private void handle(int writerThreadNo) {
		int batchSize = parameter.getWriter().getBatchSize();
		int dataQueueSize = dataQueue.size();
		List<List<Object>> data = null;
		if (readerFinish) {
			data = new ArrayList<List<Object>>(dataQueueSize);
			for (int i = 0; i < dataQueueSize; i++) {
				List<Object> objs = dataQueue.poll();
				if (null != objs) {
					data.add(objs);
				}
			}
		} else {
			if (dataQueueSize >= batchSize) {
				data = new ArrayList<List<Object>>(batchSize);
				for (int i = 0; i < batchSize; i++) {
					List<Object> objs = dataQueue.poll();
					if (null != objs) {
						data.add(objs);
					}
				}
			} else {
				// 读线程未完成且队列数量不足batchSize，线程sleep
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					LOG.error("", e);
				}
			}
		}

		if (null != data) {
			transformConsumerHandler(data, writerThreadNo);
		}
	}

	private void transformConsumerHandler(List<List<Object>> data, int writerThreadNo) {

		// TODO

		writerConsumerHandler(data, writerThreadNo);
	}

	private void writerConsumerHandler(List<List<Object>> datas, int writerThreadNo) {
//		JobParameterWriter writer = parameter.getWriter();
//		String fields = DBUtil.getFields(writer.getColumn());
//		String sql = String.format("INSERT INTO %s (%s) VALUES(%s)", writer.getTableName(), fields, values(writer.getColumn().length));
//		Connection writerConn = null;
		PreparedStatement writerPreparedStatement = null;
		try {
			Connection writerConn = writerConns.get(writerThreadNo);
			writerPreparedStatement = writerConn.prepareStatement(this.writeSql);

			
			for (int j = 0; j < datas.size(); j++) {
				List<Object> data = datas.get(j);
				for (int i = 0; i < data.size(); i++) {
					writerPreparedStatement.setObject(i + 1, data.get(i));
				}
				writerPreparedStatement.addBatch();
				
				if(j%1000==0) {
					writerPreparedStatement.executeBatch();
				}
			}

			writerPreparedStatement.executeBatch();
			writerConn.commit();

			writerMonitorNum.addAndGet(datas.size());
			
//			LOG.info("writer ok, writerThreadNo:{}", writerThreadNo);
		} catch (Exception e) {
			LOG.error("threadNo:" + threadNo + ", writerThreadNo:" + writerThreadNo, e);
		} finally {
			if (null != writerPreparedStatement) {
				try {
					writerPreparedStatement.close();
				} catch (SQLException e) {
					LOG.error("threadNo:" + threadNo + ", writerThreadNo:" + writerThreadNo, e);
				}
			}
		}
	}
	
	private void close() {
		for (int i = 0; i < writerThreadNum; i++) {
			try {
				if (null != writerConns.get(i)) {
					writerConns.get(i).close();
				}
			} catch (SQLException e) {
				LOG.error("close connection error,thread:" + threadNo, e);
			}
		}
		
	}

	private void monitor() {
		int sleepSecond = 10;
		while (!this.readerFinish || (this.readerFinish && !dataQueue.isEmpty())) {
			
			long writerNum = writerMonitorNum.getAndSet(0);
			LOG.info("threadNo:{}, dataQueueSize:{}, pk:{}, writerNum(row/s):{}", threadNo, dataQueue.size(), readerNum, writerNum / sleepSecond);

			try {
				Thread.sleep(sleepSecond * 1000);
			} catch (InterruptedException e) {
			}
		}
	}

	private String values(int num) {
		String val = "?";
		for (int i = 1; i < num; i++) {
			val += ",?";
		}
		return val;
	}

	private List<MetaData> getMetaDatas(ResultSet rs) {
		if (null == metaDatas) {
			loadMetaDatas(rs);
		}
		return metaDatas;
	}

	private synchronized void loadMetaDatas(ResultSet rs) {
		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();

			List<MetaData> metaDataList = new ArrayList<MetaData>(columnCount);

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

}
