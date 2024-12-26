package cn.easydat.etl.process.producer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.entity.TaskInfo;
import cn.easydat.etl.process.JobContainer;
import cn.easydat.etl.process.JobInfo;

public class Producer {

	private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

	private String jobNo;

	public Producer(String jobNo) {
		super();
		this.jobNo = jobNo;
	}

	public void startupAndAsynGen() {
		List<String> readerSplitSqlList = createReaderSplitSql();

		Thread readerThread = new Thread(() -> {
			genProducer(readerSplitSqlList);
		});
		readerThread.setName("readerMain");

		readerThread.start();
	}

	public List<String> createReaderSplitSql() {
		JobInfo jobInfo = JobContainer.JOB_MAP.get(jobNo);
		JobParameter parameter = jobInfo.getParameter();
		SplitTask producer = new SplitTask();
		List<String> readerSplitSqlList = producer.split(parameter);

		LOG.info("readerSplitSize size:" + readerSplitSqlList.size());

		return readerSplitSqlList;
	}

	private void genProducer(List<String> readerSplitSqlList) {
		// TODO 非命令行需要先将任务入库，再取出，并标记任务状态
		JobInfo jobInfo = JobContainer.JOB_MAP.get(jobNo);
		int channel = jobInfo.getParameter().getSetting().getChannel();

		ExecutorService executorService = Executors.newFixedThreadPool(channel);

		for (int i = 0; i < readerSplitSqlList.size(); i++) {
			TaskInfo taskInfo = new TaskInfo(i, readerSplitSqlList.get(i));
			executorService.submit(new ReaderTask(jobNo, i, taskInfo));
		}

		executorService.shutdown();

		while (!executorService.isTerminated()) {
			try {
				executorService.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.error("", e);
			}
		}

		jobInfo.setAllReaderFinish(true);
	}

}
