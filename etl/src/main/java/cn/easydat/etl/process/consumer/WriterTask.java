package cn.easydat.etl.process.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.TaskInfo;
import cn.easydat.etl.process.JobContainer;
import cn.easydat.etl.process.JobInfo;
import cn.easydat.etl.util.DBUtil;

public class WriterTask implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(WriterTask.class);

	private String jobNo;
	private int writerThreadNo;
	private JobInfo jobInfo;
	private TaskInfo taskInfo;

	private Connection conn;
	private PreparedStatement ps;

	private long writerNum;

	public WriterTask(String jobNo, int writerThreadNo) {
		super();
		this.jobNo = jobNo;
		this.writerThreadNo = writerThreadNo;
		this.jobInfo = JobContainer.JOB_MAP.get(jobNo);
		this.writerNum = 1;
	}

	@Override
	public void run() {
		Thread.currentThread().setName("WriterTask-" + jobNo);
		
		LOG.info("run init start " + writerThreadNo);
		init();
		LOG.info("run init end " + writerThreadNo);
		
		dataHandle();
	}

	private void dataHandle() {
		// 读任务未完成或 读任务已完成且队列不为空
		while (!jobInfo.isAllReaderFinish() || (jobInfo.isAllReaderFinish() && !jobInfo.dataQueueIsEmpty())) {
			Object[] data = jobInfo.dataQueueTake();
//			LOG.info("dataHandle " + data[0]);
			transformHandler(data);
		}

		Object[] data = jobInfo.dataQueueTake();
		transformHandler(data);

		commitAndClose();
	}

	private Object[] transformHandler(Object[] data) {
		// TODO

		writerHandler(data, 0);
		return data;
	}

	private void writerHandler(Object[] data, int retryTimes) {
		int batchSize = this.jobInfo.getParameter().getWriter().getBatchSize();
		if (null != data && data.length > 0) {
			try {
				for (int i = 0; i < data.length; i++) {
					this.ps.setObject(i + 1, data[i]);
				}

				ps.addBatch();

				if (this.writerNum % batchSize == 0) {
					ps.executeBatch();
					ps.clearBatch();
				}

				if (this.writerNum % (batchSize * 20) == 0) {
					this.conn.commit();
				}

				this.writerNum++;
				jobInfo.monitorWriterRowNumAdd(1);
			} catch (SQLException e) {
				LOG.error(String.format("jobNo:{},writerThreadNo:{},taskId:{}", jobNo, writerThreadNo, taskInfo.getId()), e);
				if (retryTimes < 9) {
					retryTimes++;
					writerHandler(data, retryTimes);
				} else {
					LOG.error("Exceeded the maximum number of retries.");
				}
			}
		}
	}

	private void init() {

		try {
			this.conn = DBUtil.getConnection(jobInfo.getParameter().getWriter().getJdbc());
			this.conn.setAutoCommit(false);
			this.ps = conn.prepareStatement(jobInfo.getWriteSql(),ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
		} catch (SQLException e) {
			LOG.error(String.format("jobNo:{},writerThreadNo:{},taskId:{}", jobNo, writerThreadNo, taskInfo.getId()), e);
		}
	}

	private void commitAndClose() {
		try {
			ps.executeBatch();
			ps.clearBatch();
			this.conn.commit();

			if (null != this.ps) {
				this.ps.close();
			}

			if (null != this.conn) {
				this.conn.close();
			}
		} catch (SQLException e) {
			LOG.error(String.format("jobNo:{},writerThreadNo:{},taskId:{}", jobNo, writerThreadNo, taskInfo.getId()), e);
		}
	}

}
