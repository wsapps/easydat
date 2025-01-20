package cn.easydat.etl;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.FinishInfo;
import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.process.JobContainer;
import cn.easydat.etl.process.JobInfo;
import cn.easydat.etl.process.pre.Preprocessing;
import cn.easydat.etl.process.producer.Producer;
import cn.easydat.etl.util.DBUtil;

public class EtlTaskMain {

	private static final Logger LOG = LoggerFactory.getLogger(EtlTaskMain.class);

	public FinishInfo startup(JobParameter parameter) {
		String jobNo = null != parameter.getJobNo() ? parameter.getJobNo() : UUID.randomUUID().toString().replaceAll("-", "");

		init(jobNo, parameter);

		pre(parameter);

		boolean success = producer(jobNo);

		FinishInfo finishInfo = finish(jobNo, success);
		
		return finishInfo;
	}

	private void init(String jobNo, JobParameter parameter) {
		String readerDriver = parameter.getReader().getJdbc().getDriver();
		String writerDriver = parameter.getWriter().getJdbc().getDriver();

		DBUtil.loadDriverClass(readerDriver);
		DBUtil.loadDriverClass(writerDriver);

		JobContainer.JOB_MAP.put(jobNo, new JobInfo(parameter));

		LOG.info("init finish.");
	}

	private boolean producer(String jobNo) {
		Producer producer = new Producer(jobNo);
		return producer.startupAndAsynGen();
	}

	private void pre(JobParameter parameter) {
		Preprocessing pre = new Preprocessing();
		pre.startup(parameter);
	}

	private FinishInfo finish(String jobNo, boolean success) {
		JobInfo jobInfo = JobContainer.JOB_MAP.get(jobNo);
		long endTime = System.currentTimeMillis();
		long startTime = jobInfo.getMonitorStartTime();
		long monitorProcessRowNum = 0;
		long[] monitorProcessRowNumArr = jobInfo.getMonitorProcessRowNumArr();
		for (long num : monitorProcessRowNumArr) {
			monitorProcessRowNum += num;
		}
		
		long runtime = (endTime - startTime) / 1000;

		FinishInfo finishInfo = new FinishInfo(jobNo, startTime, endTime, runtime, null, monitorProcessRowNum, success);

		JobContainer.JOB_MAP.remove(jobNo);
		LOG.info("Finish,info:{}.", finishInfo);
		return finishInfo;
	}

}
