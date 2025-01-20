package cn.easydat.etl.process.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.MetaData;
import cn.easydat.etl.entity.TaskNode;
import cn.easydat.etl.process.JobContainer;
import cn.easydat.etl.process.JobInfo;
import cn.easydat.etl.util.DBUtil;

public class Consumer implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
	private static final int RETRY_TIMES_MAX = 9;

	private String jobNo;
	private int threadNo;

	private JobInfo jobInfo;
	private TaskNode taskNode;

	private Connection writeConn;

	private BlockingQueue<Object[]> dataQueue;
	private boolean isRun;
	private int retryTimes;
	private boolean error;
	private List<Object[]> datas;

	public Consumer(String jobNo, int threadNo) {
		this.jobNo = jobNo;
		this.threadNo = threadNo;

		this.jobInfo = JobContainer.JOB_MAP.get(jobNo);
		this.taskNode = this.jobInfo.getTaskNode();

		this.dataQueue = new LinkedBlockingQueue<>(this.jobInfo.getParameter().getSetting().getMaxNumOfChannel());
		this.retryTimes = 0;
		this.datas = new ArrayList<Object[]>(this.jobInfo.getParameter().getWriter().getBatchSize());
	}

	@Override
	public void run() {
		this.isRun = true;
		Thread.currentThread().setName("Consumer-" + jobNo + "-" + threadNo);

		exe();
	}

	private void exe() {
		this.error = false;
		this.jobInfo.getMonitorProcessRowNumArr()[threadNo] = 0;

		ExecutorService executorService = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(2));
		Future<?>[] futures = new Future[2];

		futures[0] = executorService.submit(() -> {
			Thread.currentThread().setName("Consumer-" + jobNo + "-" + threadNo + "-read");
			readHandler();
		});

		futures[1] = executorService.submit(() -> {
			Thread.currentThread().setName("Consumer-" + jobNo + "-" + threadNo + "-write");
			while (this.isRun || (!this.isRun && !this.dataQueue.isEmpty())) {
				if (this.error) {
					break;
				}

				try {
					Object[] data = dataQueuePoll();
					transformHandler(data);
				} catch (SQLException e) {
					LOG.error("", e);
					this.error = true;
					this.retryTimes = 9;
					throw new RuntimeException(e);
				}
			}

			writeRetry(datas);
		});

		for (Future<?> future : futures) {
			try {
				future.get();
			} catch (Throwable e) {
				Thread.currentThread().interrupt();
				LOG.error("", e);
				this.error = true;
			}
		}

		executorService.shutdown();

		if (this.error) {
			retry();
		}
	}

	private void retry() {
		
		String readSql = taskNode.getReadSqlList().get(threadNo);

		if (this.retryTimes < RETRY_TIMES_MAX) {
			this.retryTimes++;
			this.dataQueue.clear();
			this.datas.clear();
			LOG.warn("retry:" + retryTimes);
			deleteData(0);

			exe();
		} else {
			this.dataQueue.clear();
			this.isRun = false;
			throw new RuntimeException("超过重试次数,sql:" + readSql);
		}
	}

	private void readHandler() {
		String readSql = taskNode.getReadSqlList().get(threadNo);
		LOG.info("jobNo:{}, threadNo:{}, readerSql:{}", jobNo, threadNo, readSql);
		try (Connection readerConn = DBUtil.getConnection(jobInfo.getParameter().getReader().getJdbc()); Statement stmt = createReadStatement(readerConn); ResultSet rs = stmt.executeQuery(readSql)) {
			List<MetaData> metaDataList = this.jobInfo.getMetaDatas(rs);
			int fieldSize = metaDataList.size();

			while (rs.next()) {

				if (this.error) {
					break;
				}

				Object[] data = new Object[fieldSize];
				for (int i = 0; i < fieldSize; i++) {
					data[i] = rs.getObject(i + 1);
				}

				dataQueueOffer(data);
			}

			this.isRun = false;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		LOG.info("readerHandler finish, threadNo:" + threadNo);
	}

	private Statement createReadStatement(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		return stmt;
	}

	private void deleteData(int retryTimes) {
		String deleteSql = taskNode.getDeleteSqlList().get(threadNo);
		try (Connection conn = DBUtil.getConnection(jobInfo.getParameter().getWriter().getJdbc()); Statement stmt = conn.createStatement();) {
			stmt.executeUpdate(deleteSql);
		} catch (SQLException e) {
			if (retryTimes < RETRY_TIMES_MAX) {
				retryTimes++;
				deleteData(retryTimes);
			} else {
				LOG.error("超过重试次数,deleteSql:" + deleteSql, e);
			}
		}
	}

	private Object[] transformHandler(Object[] data) throws SQLException {
		CustomTransform transform = jobInfo.getParameter().getCustomTransform();
		Object[] dataPending = data;
		if (null != transform) {
			dataPending = transform.handle(dataPending);
		}

		if (null != dataPending) {
			writeHandler(dataPending);
		}
		return data;
	}

	private void writeHandler(Object[] data) throws SQLException {
		int batchSize = this.jobInfo.getParameter().getWriter().getBatchSize();
		if (null != data && data.length > 0) {
			this.datas.add(data);

			if (this.datas.size() % batchSize == 0) {
				writeRetry(datas);
			}
		}
	}

	private void writeRetry(List<Object[]> datas) {
		int writeRetryTimes = 0;
		boolean writeException = true;
		while (writeException && writeRetryTimes < 99) {
			try {
				write(datas);
				writeException = false;
			} catch (Exception e) {
				writeException = true;
				writeRetryTimes++;
				if (writeRetryTimes % 10 == 0) {
					LOG.error("writeHandler重试次数:" + writeRetryTimes, e);
				}
			}
		}

		this.datas.clear();
	}

	private void write(List<Object[]> datas) {
		Connection conn = getWriteConn();
		try (PreparedStatement ps = conn.prepareStatement(jobInfo.getWriteSql())) {
			for (Object[] data : datas) {
				for (int i = 0; i < data.length; i++) {
					ps.setObject(i + 1, data[i]);
				}
				ps.addBatch();
			}

			ps.executeBatch();
			this.jobInfo.getMonitorProcessRowNumArr()[threadNo] = this.jobInfo.getMonitorProcessRowNumArr()[threadNo] + datas.size();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Connection getWriteConn() {
		if (null == this.writeConn) {
			this.writeConn = DBUtil.getConnection(jobInfo.getParameter().getWriter().getJdbc());
		} else {
			try {
				boolean isValid = this.writeConn.isValid(5);
				if (!isValid) {
					this.writeConn = DBUtil.getConnection(jobInfo.getParameter().getWriter().getJdbc());
				}
			} catch (SQLException e) {
				this.writeConn = DBUtil.getConnection(jobInfo.getParameter().getWriter().getJdbc());
			}
		}

		return this.writeConn;
	}

	private void dataQueueOffer(Object[] data) {
		int i = 0;
		boolean flag = false;
		while (!flag) {

			if (this.error) {
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
				LOG.warn("dataQueuePut wait " + i + ", dataQueue size:" + this.dataQueue.size());
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

}
