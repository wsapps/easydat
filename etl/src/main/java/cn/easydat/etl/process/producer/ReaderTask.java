package cn.easydat.etl.process.producer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
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

	private String jobNo;
	private int threadNo;
	private TaskInfo taskInfo;
	
	private JobInfo jobInfo;
	private JobParameter parameter;
	

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
		Thread.currentThread().setName("ReaderTask-" + jobNo + "-" + threadNo);
		readerHandler();
	}
	
	private void readerHandler() {
		
		Connection readerConn = null;
		Statement stmt = null;
		ResultSet rs = null;

		LOG.info("jobNo:{}, threadNo:{}, readerSql:{}", jobNo, threadNo, taskInfo.getSql());

		try {
			readerConn = DBUtil.getConnection(parameter.getReader().getJdbc());
			stmt = readerConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			rs = stmt.executeQuery(taskInfo.getSql());
			List<MetaData> metaDataList = this.jobInfo.getMetaDatas(rs);

			while (rs.next()) {
				int fieldSize = metaDataList.size();
				List<Object> data = new ArrayList<Object>(fieldSize);
				for (int i = 1; i <= fieldSize; i++) {
					data.add(rs.getObject(i));
				}
				
//				this.jobInfo.monitorReaderRowNumAdd(1);
				jobInfo.dataQueueOffer(data);
			}

		} catch (Exception e) {
			LOG.error("", e);
		} finally {
			DBUtil.closeDBResources(rs, stmt, readerConn);
		}
	}

}
