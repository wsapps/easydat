package cn.easydat.etl.process.producer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.entity.MetaData;
import cn.easydat.etl.entity.TaskInfo;
import cn.easydat.etl.process.JobContainer;
import cn.easydat.etl.process.JobInfo;
import cn.easydat.etl.util.DBUtil;

public class ReaderTask implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(ReaderTask.class);

	private final String jobNo;
	private final int threadNo;
	private final TaskInfo taskInfo;

	private final JobInfo jobInfo;
	private final JobParameter parameter;

	private int retryTimes = 0;
	private int row = 0;

	public ReaderTask(String jobNo, int threadNo, TaskInfo taskInfo) {
		super();
		this.jobNo = jobNo;
		this.threadNo = threadNo;
		this.taskInfo = taskInfo;
		this.jobInfo = JobContainer.JOB_MAP.get(jobNo);
		this.parameter = jobInfo.getParameter();
	}

	@Override
	public void run() {
		Thread.currentThread().setName("ReaderTask-" + jobNo);
		readerHandler();
	}

	private void readerHandler() {
		LOG.info("jobNo:{}, threadNo:{}, readerSql:{}", jobNo, threadNo, taskInfo.getSql());
		try (Connection readerConn = DBUtil.getConnection(parameter.getReader().getJdbc()); Statement stmt = createStatement(readerConn); ResultSet rs = stmt.executeQuery(taskInfo.getSql())) {
			List<MetaData> metaDataList = this.jobInfo.getMetaDatas(rs);
			int fieldSize = metaDataList.size();

			while (rs.next()) {
				Object[] data = new Object[fieldSize];
				for (int i = 0; i < fieldSize; i++) {
					data[i] = rs.getObject(i + 1);
				}

				jobInfo.dataQueuePut(data);
				jobInfo.monitorReaderRowNumAdd(1);
				row++;
			}

		} catch (SQLException e) {
			LOG.error("Database error while executing SQL [{}], jobNo: {}, threadNo: {}", taskInfo.getSql(), jobNo, threadNo, e);
			if (retryTimes < 3) {
				retryTimes++;

				jobInfo.monitorReaderRowNumAdd(-1 * row);
				row = 0;
				deleteData(0);
				readerHandler();
			} else {
				LOG.error("超过重试次数");
			}
		} catch (Exception e) {
			LOG.error("Unexpected error while executing SQL [{}], jobNo: {}, threadNo: {}", taskInfo.getSql(), jobNo, threadNo, e);
			if (retryTimes < 3) {
				retryTimes++;

				jobInfo.monitorReaderRowNumAdd(-1 * row);
				row = 0;
				deleteData(0);
				readerHandler();
			} else {
				LOG.error("超过重试次数");
			}
		}

		LOG.info("readerHandler finish, threadNo:" + threadNo);
	}

	private Statement createStatement(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		return stmt;
	}

	private void deleteData(int retryTimes) {
		try (Connection conn = DBUtil.getConnection(parameter.getWriter().getJdbc()); Statement stmt = conn.createStatement();) {
			stmt.executeUpdate(taskInfo.getDeleteSql());
		} catch (SQLException e) {
			LOG.error("Database error while executing SQL [{}], jobNo: {}, threadNo: {}, retryTimes: {}", taskInfo.getSql(), jobNo, threadNo, retryTimes, e);
			if (retryTimes < 3) {
				retryTimes++;
				deleteData(retryTimes);
			} else {
				LOG.error("超过重试次数");
			}
		}
	}

}
