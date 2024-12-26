package cn.easydat.etl;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.process.JobContainer;
import cn.easydat.etl.process.JobInfo;
import cn.easydat.etl.process.consumer.Consumer;
import cn.easydat.etl.process.monitor.Monitor;
import cn.easydat.etl.process.pre.Preprocessing;
import cn.easydat.etl.process.producer.Producer;
import cn.easydat.etl.util.DBUtil;

public class Main {

	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	public void startup(JobParameter parameter) {
		String jobNo = UUID.randomUUID().toString().replaceAll("-", "");

		init(jobNo, parameter);

		producer(jobNo);

		startupMonitor(jobNo);
		
		pre(parameter);

		createConsumer(jobNo);
	}

	private void init(String jobNo, JobParameter parameter) {
		String readerDriver = parameter.getReader().getJdbc().getDriver();
		String writerDriver = parameter.getWriter().getJdbc().getDriver();

		DBUtil.loadDriverClass(readerDriver);
		DBUtil.loadDriverClass(writerDriver);

		JobContainer.JOB_MAP.put(jobNo, new JobInfo(parameter));

		LOG.info("init finish.");
	}

	private void producer(String jobNo) {
		Producer producer = new Producer(jobNo);
		producer.startupAndAsynGen();
	}

	private void startupMonitor(String jobNo) {
		Monitor monitor = new Monitor(jobNo);
		monitor.startup();
	}
	
	private void pre(JobParameter parameter) {
		Preprocessing pre = new Preprocessing();
		pre.startup(parameter);
	}

	private void createConsumer(String jobNo) {
		Consumer consumer = new Consumer(jobNo);
		consumer.startup();
	}

}
