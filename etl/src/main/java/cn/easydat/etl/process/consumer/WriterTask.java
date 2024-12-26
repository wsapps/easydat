package cn.easydat.etl.process.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

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
		this.writerNum = 0;
	}

	@Override
	public void run() {
		Thread.currentThread().setName("WriterTask-" + jobNo + "-" + writerThreadNo);

		init();

		dataHandle();
	}

	private void dataHandle() {
		while (!jobInfo.isAllReaderFinish()) {
			List<Object> data = jobInfo.dataQueuePollBlock();
			transformHandler(data);
		}

		List<Object> data = jobInfo.dataQueuePollBlock();
		transformHandler(data);

		commitAndClose();
	}

	private List<Object> transformHandler(List<Object> data) {
		// TODO

		writerHandler(data);
		return data;
	}

	private void writerHandler(List<Object> data) {
		this.writerNum++;
		int batchSize = this.jobInfo.getParameter().getWriter().getBatchSize();
		if (null != data && !data.isEmpty()) {
			try {
				for (int i = 0; i < data.size(); i++) {
					this.ps.setObject(i + 1, data.get(i));
				}

				ps.addBatch();
				jobInfo.monitorWriterRowNumAdd(1);

				if (this.writerNum % batchSize == 0) {
					ps.executeBatch();
					ps.clearBatch();
				}

				if (this.writerNum % (batchSize * 20) == 0) {
					this.conn.commit();
				}

			} catch (SQLException e) {
				LOG.error(String.format("jobNo:{},writerThreadNo:{},taskId:{}", jobNo, writerThreadNo, taskInfo.getId()), e);
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
